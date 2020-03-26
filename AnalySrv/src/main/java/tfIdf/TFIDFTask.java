package tfIdf;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;


public class TFIDFTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        //计算每个文本数据内每个词条数量，将计算的数据存入对象内返回，并将所有词条存入HBase
        DataSet<TfIdfEntity> mapresult = text.map(new IdfMap());
        //聚合计算所有文本内的总词条数量
        DataSet<TfIdfEntity> reduceresult = mapresult.reduce(new IdfReduce());

        Long totaldoucment = 0l;
        try {
            totaldoucment = reduceresult.collect().get(0).getTotaldocumet();
            DataSet<TfIdfEntity> mapfinalresult = mapresult.map(new IdfMapfinal(totaldoucment,3));
            mapfinalresult.writeAsText("hdfs://tfidf/test");//hdfs的路径
            env.execute("TFIDFTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
