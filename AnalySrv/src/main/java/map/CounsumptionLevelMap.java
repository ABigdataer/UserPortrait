package map;

import entity.ConsumptionLevel;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;


public class CounsumptionLevelMap implements MapFunction<String,ConsumptionLevel>{
    @Override
    public ConsumptionLevel map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] orderinfos = s.split(",");
        String id= orderinfos[0];
        String productid = orderinfos[1];
        String producttypeid = orderinfos[2];
        String createtime = orderinfos[3];
        String amount = orderinfos[4];
        String paytype = orderinfos[5];
        String paytime = orderinfos[6];
        String paystatus = orderinfos[7];
        String couponamount = orderinfos[8];
        String totalamount = orderinfos[9];
        String refundamount = orderinfos[10];
        String num = orderinfos[11];
        String userid = orderinfos[12];

        ConsumptionLevel consumptionLevel = new ConsumptionLevel();
        consumptionLevel.setUserid(userid);
        consumptionLevel.setAmounttotaol(totalamount);
        consumptionLevel.setGroupfield("=== consumptionLevel=="+userid);

        return consumptionLevel;
    }
}
