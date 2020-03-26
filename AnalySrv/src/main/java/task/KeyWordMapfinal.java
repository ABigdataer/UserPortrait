package task;

import entity.KeyWordEntity;
import util.HbaseUtils;
import util.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.*;


public class KeyWordMapfinal implements MapFunction<KeyWordEntity, KeyWordEntity> {

    private long totaldoucments = 0l;
    private long words;
    private String columnName;
    public KeyWordMapfinal(long totaldoucments, long words,String columnName){
        this.totaldoucments = totaldoucments;
        this.words = words;
        this.columnName = columnName;

    }

    @Override
    public KeyWordEntity map(KeyWordEntity keyWordEntity) throws Exception {

        Map<String,Double> tfidfmap = new HashMap<String,Double>();
        String userid = keyWordEntity.getUserid();
        Map<String,Double> tfmap = keyWordEntity.getTfmap();
        Set<Map.Entry<String,Double>> set = tfmap.entrySet();

        String tablename = "keyworddata";
        String famliyname="baseinfo";
        String colum="idfcount";

        //计算tf-idf
        for(Map.Entry<String,Double> entry:set){
            String word = entry.getKey();
            Double value = entry.getValue();
            String data = HbaseUtils.getdata(tablename,word,famliyname,colum);
            long viewcount = Long.valueOf(data);
            Double idf = Math.log(totaldoucments/viewcount+1);
            Double tfidf = value*idf;
            tfidfmap.put(word,tfidf);
        }

        LinkedHashMap<String,Double> resultfinal = MapUtils.sortMapByValue(tfidfmap);
        Set<Map.Entry<String,Double>> entryset = resultfinal.entrySet();
        List<String> finalword = new ArrayList<String>();
        int count =1;
        for(Map.Entry<String,Double> mapentry:entryset){
            finalword.add(mapentry.getKey());
            count++;
            if(count>words){
                break;
            }
        }
        KeyWordEntity keyWordEntityfinal = new KeyWordEntity();
        keyWordEntityfinal.setUserid(userid);
        keyWordEntityfinal.setFinalkeyword(finalword);

        String keywordstring= "";
        for(String keyword:finalword){
            keywordstring += keyword+",";
        }
        if (StringUtils.isNotBlank(keywordstring)){
            String tablename1 = "userkeywordlabel";
            String rowkey1=userid;
            String famliyname1="baseinfo";
            String colum1=columnName;
            HbaseUtils.putdata(tablename1,rowkey1,famliyname1,colum1,keywordstring);
        }
        return keyWordEntityfinal;
    }
}
