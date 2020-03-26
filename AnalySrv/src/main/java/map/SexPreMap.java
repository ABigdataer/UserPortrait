package map;

import entity.SexPreInfo;
import org.apache.flink.api.common.functions.MapFunction;
import java.util.Random;

public class SexPreMap implements MapFunction<String, SexPreInfo> {
    @Override
    public SexPreInfo map(String s) throws Exception {
        String[] temps = s.split("\t");
        Random random = new Random();
        int userid = Integer.valueOf(temps[0]);
        long ordernum = Long.valueOf(temps[1]);//订单的总数
        long orderfre = Long.valueOf(temps[4]);//隔多少天下单
        int manclothes =Integer.valueOf(temps[5]);//浏览男装次数
        int womenclothes = Integer.valueOf(temps[6]);//浏览女装的次数
        int childclothes = Integer.valueOf(temps[7]);//浏览小孩衣服的次数
        int oldmanclothes = Integer.valueOf(temps[8]);//浏览老人的衣服的次数
        double avramount = Double.valueOf(temps[9]);//订单平均金额
        int producttimes = Integer.valueOf(temps[10]);//每天浏览商品数
        int label = Integer.valueOf(temps[11]);//0男，1女
        String fieldgroup = "sexpre=="+random.nextInt(10);//随机分组
        SexPreInfo sexPreInfo = new SexPreInfo();
        sexPreInfo.setUserid(userid);
        sexPreInfo.setOrdernum(ordernum);
        sexPreInfo.setOrderfre(orderfre);
        sexPreInfo.setManclothes(manclothes);
        sexPreInfo.setWomenclothes(womenclothes);
        sexPreInfo.setChildclothes(childclothes);
        sexPreInfo.setOldmanclothes(oldmanclothes);
        sexPreInfo.setAvramount(avramount);
        sexPreInfo.setProducttimes(producttimes);
        sexPreInfo.setLabel(label);
        sexPreInfo.setGroupfield(fieldgroup);
        return sexPreInfo;
    }
}
