package tfIdf;

import util.HbaseUtils;
import util.IkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import java.util.*;

/**
 *一段文本
 */
public class IdfMap implements MapFunction<String, TfIdfEntity> {

    @Override
    public TfIdfEntity map(String s) throws Exception {

        //存储词条和对应的数量
        Map<String,Long> tfmap = new HashMap<String,Long>();
        //存储该文本内所有词条
        Set<String> wordset = new HashSet<String>();

        //将文本进行分词
        List<String> listdata = IkUtil.getIkWord(s);
        //计算分此后所有词条的个数
        for(String word:listdata){
            Long pre = tfmap.get(word)==null?0l:tfmap.get(word);
            tfmap.put(word,pre+1);
            wordset.add(word);
        }

        //产生随机数，当作文本ID
        String docuemtnid = UUID.randomUUID().toString();
        TfIdfEntity tfIdfEntity = new TfIdfEntity();
        tfIdfEntity.setDocumentid(docuemtnid);
        tfIdfEntity.setDatamap(tfmap);

        //计算所有词条个数的总数
        long sum = 0l;
        Collection<Long> longset = tfmap.values();
        for(Long templong:longset){
                sum += templong;
        }

        //计算每个词条在该文本数据内的TF
        Map<String,Double> tfmapfinal = new HashMap<String,Double>();
        Set<Map.Entry<String,Long>> entryset = tfmap.entrySet();
        for(Map.Entry<String,Long> entry:entryset){
                String word = entry.getKey();
                long count = entry.getValue();
                double tf = Double.valueOf(count)/Double.valueOf(sum);
                tfmapfinal.put(word,tf);
        }
        tfIdfEntity.setTfmap(tfmapfinal);

        //将所有词条存入HBase
        //create "tfidfdata,"baseinfo"
        for(String word:wordset){
            String tablename = "tfidfdata";
            String rowkey=word;
            String famliyname="baseinfo";
            String colum="idfcount";
            String data = HbaseUtils.getdata(tablename,rowkey,famliyname,colum);
            Long pre = data==null?0l:Long.valueOf(data);
            Long total = pre+1;
            HbaseUtils.putdata(tablename,rowkey,famliyname,colum,total+"");
        }
        return tfIdfEntity;
    }
}
