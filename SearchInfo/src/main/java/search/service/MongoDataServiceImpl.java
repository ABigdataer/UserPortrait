package search.service;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import entity.AnalyResult;
import org.bson.Document;
import org.springframework.stereotype.Service;
import search.base.BaseMongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Service
public class MongoDataServiceImpl extends BaseMongo {


    public List<AnalyResult> listMongoInfoby(String tablename) {
        List<AnalyResult> result = new ArrayList<AnalyResult>();


        MongoDatabase db = mongoClient.getDatabase("Portrait");
        MongoCollection<Document> collection =  db.getCollection(tablename);


        Document groupFields = new Document();
        Document idFields = new Document();
        idFields.put("info", "$info");
        groupFields.put("_id", idFields);
        groupFields.put("count", new Document("$sum", "$count"));

        Document group = new Document("$group", groupFields);


        Document projectFields = new Document();
        projectFields.put("_id", false);
        projectFields.put("info", "$_id.info");
        projectFields.put("count", true);
        Document project = new Document("$project", projectFields);
        AggregateIterable<Document> iterater = collection.aggregate(
                (List<Document>) Arrays.asList(group, project)
        );

        MongoCursor<Document> cursor = iterater.iterator();
        while(cursor.hasNext()){
            Document document = cursor.next();
            String jsonString = JSONObject.toJSONString(document);
            AnalyResult brandUser = JSONObject.parseObject(jsonString,AnalyResult.class);
            result.add(brandUser);
        }
        return result;
    }
}
