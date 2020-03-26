package task;

import entity.ConsumptionLevel;
import map.CounsumptionLevelMap;
import reduce.ConsumptionLeaveFinalReduce;
import reduce.ConsumptionLevelReduce;
import util.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;
import java.util.List;

/**
 * 消费水平标签
 */
public class ConsumptionLevelTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        //获取用户每个订单的总消费金额
        DataSet<ConsumptionLevel> mapresult = text.map(new CounsumptionLevelMap());
        //按照用户ID进行分组，计算每个用户的消费水平，并将消费等级数据更新到HBase，返回用户消费等级数据
        DataSet<ConsumptionLevel> reduceresult = mapresult.groupBy("groupfield").reduceGroup(new ConsumptionLevelReduce());
        //按照用户消费等级进行分类，最后求得每个消费等级的人数，以便总体消费水平
        DataSet<ConsumptionLevel> reduceresultfinal = reduceresult.groupBy("groupfield").reduce(new ConsumptionLeaveFinalReduce());

        //将每个消费等级的总人数数据更新到MongoDB
        try {
            List<ConsumptionLevel> reusltlist = reduceresultfinal.collect();
            for(ConsumptionLevel consumptionLevel:reusltlist){
                String consumptiontype = consumptionLevel.getConsumptiontype();
                Long count = consumptionLevel.getCount();
                Document doc = MongoUtils.findoneby("consumptionlevelstatics","Portrait",consumptiontype);
                if(doc == null){
                    doc = new Document();
                    doc.put("info",consumptiontype);
                    doc.put("count",count);
                }else{
                    Long countpre = doc.getLong("count");
                    Long total = countpre+count;
                    doc.put("count",total);
                }
                MongoUtils.saveorupdatemongo("consumptionlevelstatics","Portrait",doc);
            }
            env.execute("ConsumptionLevelTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
