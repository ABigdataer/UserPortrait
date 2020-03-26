package map;

import entity.KeyWordEntity;
import util.HbaseUtils;
import util.IkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import java.util.*;

public class KeywordMap2 implements MapFunction<KeyWordEntity, KeyWordEntity> {

    @Override
    public KeyWordEntity map(KeyWordEntity keyWordEntity) throws Exception {

        //取出当前用户所有文本数据
        List<String> words = keyWordEntity.getOriginalwords();

        //存储词条和对应的词条数量
        Map<String,Long> tfmap = new HashMap<String,Long>();
        //存储所有词条
        Set<String> wordset = new HashSet<String>();
        for(String outerword:words){
            //分词
            List<String> listdata = IkUtil.getIkWord(outerword);
            for(String word:listdata){
                Long pre = tfmap.get(word)==null?0l:tfmap.get(word);
                tfmap.put(word,pre+1);
                wordset.add(word);
            }
        }

        KeyWordEntity keyWordEntityfinal = new KeyWordEntity();
        String userid = keyWordEntity.getUserid();
        keyWordEntityfinal.setUserid(userid);
        keyWordEntityfinal.setDatamap(tfmap);

        //计算所有词条个数的总数
        long sum = 0l;
        Collection<Long> longset = tfmap.values();
        for(Long templong:longset){
            sum += templong;
        }

        Map<String,Double> tfmapfinal = new HashMap<String,Double>();
        //计算所有词条的TF
        Set<Map.Entry<String,Long>> entryset = tfmap.entrySet();
        for(Map.Entry<String,Long> entry:entryset){
            String word = entry.getKey();
            long count = entry.getValue();
            double tf = Double.valueOf(count)/Double.valueOf(sum);
            tfmapfinal.put(word,tf);
        }
        keyWordEntityfinal.setTfmap(tfmapfinal);

        //将所有词条存入HBase
        //create "keyworddata,"baseinfo"
        for(String word:wordset){
            String tablename = "keyworddata";
            String rowkey=word;
            String famliyname="baseinfo";
            String colum="idfcount";
            String data = HbaseUtils.getdata(tablename,rowkey,famliyname,colum);
            Long pre = data==null?0l:Long.valueOf(data);
            Long total = pre+1;
            HbaseUtils.putdata(tablename,rowkey,famliyname,colum,total+"");
        }
        return keyWordEntity;
    }
}
