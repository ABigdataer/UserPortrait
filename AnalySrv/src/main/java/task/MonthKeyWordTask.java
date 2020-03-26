package task;

import entity.KeyWordEntity;
import map.KeywordMap;
import map.KeywordMap2;
import reduce.KeyWordReduce2;
import reduce.KeywordReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;


/**
 * 计算每个用户访问商品属性数据内每个词条数量
 * 聚合计算所有用户文本内的总词条数量
 */
public class MonthKeyWordTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        //返回用户ID和所有商品属性数据
        DataSet<KeyWordEntity> mapresult = text.map(new KeywordMap());
        //按照用户ID分组后，将同一用户浏览的商品属性数据聚合到同一个对象内
        DataSet<KeyWordEntity> reduceresutl = mapresult.groupBy("userid").reduce(new KeywordReduce());
        //计算每个用户的文本数据内每个词条数量，将计算的数据存入对象内返回，并将所有词条存入HBase
        DataSet<KeyWordEntity> mapresult2 = reduceresutl.map(new KeywordMap2());
        //聚合计算所有用户文本内的总词条数量
        DataSet<KeyWordEntity> reduceresult2 = mapresult2.reduce(new KeyWordReduce2());
        Long totaldoucment = 0l;
            try {
            totaldoucment = reduceresult2.collect().get(0).getTotaldocumet();
            DataSet<KeyWordEntity> mapfinalresult = mapresult.map(new KeyWordMapfinal(totaldoucment,3,"month"));
            mapfinalresult.writeAsText("hdfs://tfidf/test/month");//hdfs的路径
            env.execute("MonthrKeyWordTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
