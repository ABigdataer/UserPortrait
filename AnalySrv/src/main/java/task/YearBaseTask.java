package task;

import entity.YearBase;
import map.YearBaseMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;
import reduce.YearBaseReduce;
import util.MongoUtils;

import java.util.List;

/**
 * 年代标签
 *      年代：40年代 50年代 60年代 70年代 80年代 90年代 00后 10后
 *      统计每个年代群里的数量，做到近实时统计，每半小时会进行一次任务统计
 */
public class YearBaseTask {

    public static void main(String[] args) throws Exception{

        final ParameterTool params = ParameterTool.fromArgs(args);

        final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        //设置全局参数
        environment.getConfig().setGlobalJobParameters(params);

        //读取数据
        DataSet<String> text = environment.readTextFile(params.get("input"));

        DataSet<YearBase> mapresult = text.map(new YearBaseMap());
        DataSet<YearBase> reduceresult = mapresult.groupBy("groupfeild").reduce(new YearBaseReduce());
        List<YearBase> resultlist = reduceresult.collect();
        for (YearBase yearBase : resultlist)
        {
            String yeartype = yearBase.getYeartype();
            Long count = yearBase.getCount();
            Document doc = MongoUtils.findoneby("yearbasestatics", "Portrait", yeartype);

            if (doc == null)
            {
              doc = new Document();
              doc.put("info", yeartype);
              doc.put("count", count);
            }else
            {
               Long counttpre = doc.getLong("count");
               Long total = counttpre + count;
               doc.put("count", total);
            }
            MongoUtils.saveorupdatemongo("yearbasestatics", "Portrait",doc);
        }

        //执行代码
        environment.execute("YearBaseTask Runing");
    }


}
