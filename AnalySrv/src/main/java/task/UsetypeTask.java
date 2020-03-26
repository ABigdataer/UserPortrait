package task;

import entity.UseTypeInfo;
import kafka.KafkaEvent;
import kafka.KafkaEventSchema;
import map.UseTypeMap;
import reduce.UseTypeReduce;
import reduce.UseTypeSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import javax.annotation.Nullable;

/**
 * 实时计算用户终端偏好标签
 */
public class UsetypeTask {
    public static void main(String[] args) {
        // parse input arguments
        args = new String[]{"--input-topic","scanProductLog","--bootstrap.servers","192.168.80.134:9092","--zookeeper.connect","192.168.80.134:2181","--group.id","youfan"};
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         *禁止JobManager状态更新打印到System.out
         */
        env.getConfig().disableSysoutLogging();

        /**
         * Flink支持不同的重启策略，以在故障发生时控制作业如何重启
         * 重启策略可以在flink-conf.yaml中配置，表示全局的配置。也可以在应用代码中动态指定，会覆盖全局配置
         * 常用的重启策略：固定间隔 (Fixed delay)，失败率 (Failure rate)和无重启 (No restart)
         *      如果没有启用 checkpointing，则使用无重启 (no restart) 策略。
         *      如果启用了 checkpointing，但没有配置重启策略，则使用固定间隔 (fixed-delay) 策略，其中 Integer.MAX_VALUE 参数是尝试重启次数
         */
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));//设置尝试重启次数和时间间隔

        /**
         *      Checkpoint是Flink实现容错机制最核心的功能，它能够根据配置周期性地基于Stream中各个Operator/task的状态来生成快照，从而将这些状态数据定期持久化存储下来，
         * 当Flink程序一旦意外崩溃时，重新运行程序时可以有选择地从这些快照进行恢复，从而修正因为故障带来的程序数据异常
         *      默认checkpoint功能是disabled的，想要使用的时候需要先启用
         *      checkpoint的checkPointMode有两种:Exactly-once和At-least-once,checkpoint开启之后，默认的checkPointMode是Exactly-once,Exactly-once对于大多
         * 数应用来说是最合适的。At-least-once可能用在某些延迟超低的应用程序（始终延迟为几毫秒）
         */
        env.enableCheckpointing(5000); //// 每隔5000ms进行启动一个检查点【设置checkpoint的周期】

        env.getConfig().setGlobalJobParameters(parameterTool); // 设置全局化参数

        /**
         * 针对stream数据中的时间，可以分为以下三种
         *      Event Time：事件产生的时间，它通常由事件中的时间戳描述。
         *      Ingestion time：事件进入Flink的时间
         *      Processing Time：事件被处理时当前系统的时间
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//设置Time类型

        DataStream<KafkaEvent> input = env
                .addSource(
                        /**
                         * KafkaConsumer参数解析：
                         * 1. topic名或 topic名的列表
                         * 2. 反序列化约束，以便于Flink决定如何反序列化从Kafka获得的数据
                         * 3. Kafka consumer的属性配置，下面两个属性配置是必须的:
                         * · “zookeeper.connect” (Zookeeper servers的地址列表，以逗号分隔)
                         * · “group.id” (consumer group)
                         * · “bootstrap.servers” (Kafka brokers的地址列表，以逗号分隔)
                         */
                        new FlinkKafkaConsumer010<>
                                (
                                parameterTool.getRequired("input-topic"),
                                //Flink Kafka Consumer 需要知道如何将来自Kafka的二进制数据转换为Java/Scala对象。DeserializationSchema接口允许程序员指定这个序列化的实现。
                                new KafkaEventSchema(),
                                parameterTool.getProperties()
                                )
                                //添加水位线，解决数据时间乱序问题
                                .assignTimestampsAndWatermarks(new CustomWatermarkExtractor()));

        DataStream<UseTypeInfo> useTypeMap = input.flatMap(new UseTypeMap());

        DataStream<UseTypeInfo> useTypeReduce = useTypeMap.keyBy("groupbyfield").timeWindowAll(Time.seconds(2)).reduce(new UseTypeReduce());

        useTypeReduce.addSink(new UseTypeSink());

        try {
            env.execute("useType analy");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *      在使用eventTime的时候如何处理乱序数据？
     *      我们知道，流处理从事件产生，到流经source，再到operator，中间是有一个过程和时间的。虽然大部分情况下，流到operator的数据都是按照事件产生的时间顺序来的，
     * 但是也不排除由于网络延迟等原因，导致乱序的产生，特别是使用kafka的话，多个分区的数据无法保证有序。所以在进行window计算的时候，我们又不能无限期的等下去，必须要
     * 有个机制来保证一个特定的时间后，必须触发window去进行计算了。这个特别的机制，就是watermark，watermark是用于处理乱序事件的。
     *      watermark可以翻译为水位线，用来处理事件乱序问题
     */
    private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent> {

        private static final long serialVersionUID = -742759155861320823L;

        private long currentTimestamp = Long.MIN_VALUE;

        /**
         * extractTimestamp 方法是从数据本身中提取EventTime
         * @param event
         * @param previousElementTimestamp
         * @return
         */
        @Override
        public long extractTimestamp(KafkaEvent event, long previousElementTimestamp) {
            // the inputs are assumed to be of format (message,timestamp)
            this.currentTimestamp = event.getTimestamp();
            return event.getTimestamp();
        }

        /**
         * getCurrentWatermar 方法是获取当前水位线
         * @return
         */
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }
    }
}
