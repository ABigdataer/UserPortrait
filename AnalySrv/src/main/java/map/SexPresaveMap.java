package map;

import entity.SexPreInfo;
import logic.Logistic;
import util.HbaseUtils;
import org.apache.flink.api.common.functions.MapFunction;
import java.util.ArrayList;
import java.util.Random;

public class SexPresaveMap implements MapFunction<String, SexPreInfo> {

    private ArrayList<Double> weights = null;
    public SexPresaveMap(ArrayList<Double> weights){
            this.weights = weights;
    }

    @Override
    public SexPreInfo map(String s) throws Exception {
        String[] temps = s.split("\t");
        Random random = new Random();
        //清洗以及归一化
        int userid = Integer.valueOf(temps[0]);
        long ordernum = Long.valueOf(temps[1]);//订单的总数
        long orderfre = Long.valueOf(temps[4]);//隔多少天下单
        int manclothes =Integer.valueOf(temps[5]);//浏览男装次数
        int womenclothes = Integer.valueOf(temps[6]);//浏览女装的次数
        int childclothes = Integer.valueOf(temps[7]);//浏览小孩衣服的次数
        int oldmanclothes = Integer.valueOf(temps[8]);//浏览老人的衣服的次数
        double avramount = Double.valueOf(temps[9]);//订单平均金额
         int producttimes = Integer.valueOf(temps[10]);//每天浏览商品数

        ArrayList<String> as = new ArrayList<String>();
        as.add(ordernum+"");
        as.add(orderfre+"");
        as.add(manclothes+"");
        as.add(womenclothes+"");
        as.add(childclothes+"");
        as.add(oldmanclothes+"");
        as.add(avramount+"");
        as.add(producttimes+"");

        String sexflag = Logistic.classifyVector(as, weights);
        String sexstring = sexflag=="0"?"女":"男";

        String tablename = "userflaginfo";
        String rowkey = userid+"";
        String famliyname = "baseinfo";
        String colum = "sex";//运营商
        HbaseUtils.putdata(tablename,rowkey,famliyname,colum,sexstring);

        return null;
    }
}
