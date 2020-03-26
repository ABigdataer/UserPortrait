package task;

import entity.SexPreInfo;
import map.SexPreMap;
import map.SexPresaveMap;
import reduce.SexpreReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import java.util.*;

/**
 * 逻辑回归算法性别预测
 */
public class SexPreTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        //将数据随机分组
        DataSet<SexPreInfo> mapresult = text.map(new SexPreMap());

        /**
         * 【reduce】
         * 应用于分组DataSet的Reduce转换使用用户定义的reduce函数将每个组减少为单个元素。
         * 对于每组输入元素，reduce函数连续地将元素对组合成一个元素，直到每个组只剩下一个元素。
         * 【reduceGroup】
         * 应用于分组DataSet的GroupReduce调用用户定义的group-reduce函数转换每个分组。
         * 与Reduce的区别在于用户定义的函数会立即获得整个组，在组的所有元素上使用Iterable调用该函数，并且可以返回任意数量的结果元素。
         * 计算权值
         */
        DataSet<ArrayList<Double>> reduceresutl = mapresult.groupBy("groupfield").reduceGroup(new SexpreReduce());

        try {
            List<ArrayList<Double>> reusltlist = reduceresutl.collect();
            int groupsize  = reusltlist.size();
            Map<Integer,Double> summap = new TreeMap<Integer,Double>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o1.compareTo(o2);
                }
            });
            for(ArrayList<Double> array:reusltlist){

                for(int i=0;i<array.size();i++){
                    double pre = summap.get(i)==null?0d:summap.get(i);
                    summap.put(i,pre+array.get(i));
                }
            }
            ArrayList<Double> finalweight = new ArrayList<Double>();
            Set<Map.Entry<Integer,Double>> set = summap.entrySet();
            for(Map.Entry<Integer,Double> mapentry :set){
                Integer key = mapentry.getKey();
                Double sumvalue = mapentry.getValue();
                double finalvalue = sumvalue/groupsize;
                finalweight.add(finalvalue);
            }

            DataSet<String> text2 = env.readTextFile(params.get("input2"));
            text2.map(new SexPresaveMap(finalweight));

            env.execute("sexPreTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
