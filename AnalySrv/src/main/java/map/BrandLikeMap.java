package map;

import com.alibaba.fastjson.JSONObject;
import entity.BrandLike;
import kafka.KafkaEvent;
import log.ScanProductLog;
import util.HbaseUtils;
import utils.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Map;

public class BrandLikeMap implements FlatMapFunction<KafkaEvent, BrandLike>  {

    private static final long serialVersionUID = 3943962864585875085L;

    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<BrandLike> collector) throws Exception {

            String data = kafkaEvent.getWord();//获取数据

            ScanProductLog scanProductLog = JSONObject.parseObject(data,ScanProductLog.class);
            int userid = scanProductLog.getUserid();//用户ID
            String brand = scanProductLog.getBrand();//品牌

            String tablename = "userflaginfo";
            String rowkey = userid+"";
            String famliyname = "userbehavior";
            String colum = "brandlist";//运营
            //获取运营商信息
            String mapdata = HbaseUtils.getdata(tablename,rowkey,famliyname,colum);
            Map<String,Long> map = new HashMap<String,Long>();
            if(StringUtils.isNotBlank(mapdata)){
                map = JSONObject.parseObject(mapdata,Map.class);
            }

            //获取之前的品牌偏好
            String maxprebrand = MapUtils.getmaxbyMap(map);

            long prebarnd = map.get(brand)==null?0l:map.get(brand);
            map.put(brand,prebarnd+1);
            String finalstring = JSONObject.toJSONString(map);
            HbaseUtils.putdata(tablename,rowkey,famliyname,colum,finalstring);

            String maxbrand = MapUtils.getmaxbyMap(map);
            if(StringUtils.isNotBlank(maxbrand)&&!maxprebrand.equals(maxbrand)){
                BrandLike brandLike = new BrandLike();
                brandLike.setBrand(maxprebrand);
                brandLike.setCount(-1l);
                brandLike.setGroupbyfield("==brandlik=="+maxprebrand);
                collector.collect(brandLike);
            }

            BrandLike brandLike = new BrandLike();
            brandLike.setBrand(maxbrand);
            brandLike.setCount(1l);
            brandLike.setGroupbyfield("==brandlik=="+maxbrand);
            collector.collect(brandLike);

            colum = "brandlike";
            HbaseUtils.putdata(tablename,rowkey,famliyname,colum,maxbrand);

    }

}
