package reduce;

import entity.UseTypeInfo;
import util.MongoUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

public class UseTypeSink implements SinkFunction<UseTypeInfo> {
    @Override
    public void invoke(UseTypeInfo value, Context context) throws Exception {
        String usetype = value.getUsetype();
        long count = value.getCount();
        Document doc = MongoUtils.findoneby("usetypestatics","youfanPortrait",usetype);
        if(doc == null){
            doc = new Document();
            doc.put("info",usetype);
            doc.put("count",count);
        }else{
            Long countpre = doc.getLong("count");
            Long total = countpre+count;
            doc.put("count",total);
        }
        MongoUtils.saveorupdatemongo("usetypestatics","youfanPortrait",doc);
    }
}
