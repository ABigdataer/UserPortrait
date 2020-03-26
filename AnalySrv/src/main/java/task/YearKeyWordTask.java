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
 * 统计每个年代群里的数量
 * 并计算每个词条的TF值
 */
public class YearKeyWordTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<KeyWordEntity> mapresult = text.map(new KeywordMap());

        DataSet<KeyWordEntity> reduceresutl = mapresult.groupBy("userid").reduce(new KeywordReduce());

        DataSet<KeyWordEntity> mapresult2 = reduceresutl.map(new KeywordMap2());

        DataSet<KeyWordEntity> reduceresult2 = mapresult2.reduce(new KeyWordReduce2());

        Long totaldoucment = 0l;
        try {
            totaldoucment = reduceresult2.collect().get(0).getTotaldocumet();
            DataSet<KeyWordEntity> mapfinalresult = mapresult.map(new KeyWordMapfinal(totaldoucment,3,"year"));
            mapfinalresult.writeAsText("hdfs://tfidf/test/year");//hdfs的路径
            env.execute("YearKeyWordTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
