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
 * 计算每个商品属性词条的TF-IDF
 */
public class QuarterKeyWordTask {
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

        //计算每个用户访问的所有商品属性词条访问次数以及关键词的TF
        DataSet<KeyWordEntity> mapresult2 = reduceresutl.map(new KeywordMap2());

        //计算所有用户访问的所有商品属性词条访问总数
        DataSet<KeyWordEntity> reduceresult2 = mapresult2.reduce(new KeyWordReduce2());

        //计算每个词条的TF-IDF
        Long totaldoucment = 0l;
        try {
            totaldoucment = reduceresult2.collect().get(0).getTotaldocumet();
            DataSet<KeyWordEntity> mapfinalresult = mapresult.map(new KeyWordMapfinal(totaldoucment,3,"quarter"));
            mapfinalresult.writeAsText("hdfs://tfidf/test/quarter");//hdfs的路径
            env.execute("QuarterKeyWordTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
