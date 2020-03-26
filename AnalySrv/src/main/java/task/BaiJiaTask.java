package task;

import entity.BaiJiaInfo;
import map.BaijiaMap;
import reduce.BaijiaReduce;
import util.DateUtils;
import util.HbaseUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *败家指数 = 支付金额平均值*0.3、最大支付金额*0.3、下单频率*0.4
 *用户订单表，商品表，商品类别表
 */
public class BaiJiaTask {

    public static void main(String[] args) {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<BaiJiaInfo> mapresult = text.map(new BaijiaMap());
        DataSet<BaiJiaInfo> reduceresutl = mapresult.groupBy("groupfield").reduce(new BaijiaReduce());

        try {
            List<BaiJiaInfo> reusltlist = reduceresutl.collect();
            for(BaiJiaInfo baiJiaInfo:reusltlist)
            {
                String userid = baiJiaInfo.getUserid();
                List<BaiJiaInfo> list = baiJiaInfo.getList();
                //按照订单创建时间排序
                Collections.sort(list, new Comparator<BaiJiaInfo>() {
                        @Override
                        public int compare(BaiJiaInfo o1, BaiJiaInfo o2) {
                            String timeo1 = o1.getCreatetime();
                            String timeo2 = o2.getCreatetime();
                            DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd hhmmss");
                            Date datenow = new Date();
                            Date time1 = datenow;
                            Date time2 = datenow;
                            try {
                                time1 = dateFormat.parse(timeo1);
                                time2 = dateFormat.parse(timeo2);
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            return time1.compareTo(time2);
                        }
                    });

                BaiJiaInfo before = null;
                Map<Integer,Integer> frequencymap = new HashMap<Integer,Integer>();
                double maxamount = 0d;//最大消费金额
                double sum = 0d;//总消费金额
                for(BaiJiaInfo baiJiaInfoinner:list)
                {
                    if(before==null){
                        before = baiJiaInfoinner;
                        continue;
                    }
                    //计算购买的频率，隔多少天购买一次商品
                    String beforetime = before.getCreatetime();
                    String endstime = baiJiaInfoinner.getCreatetime();
                    //计算两次购买相差多少天
                    int days = DateUtils.getDaysBetweenbyStartAndend(beforetime,endstime,"yyyyMMdd hhmmss");
                    //第几次购买行为
                    int brefore = frequencymap.get(days)==null?0:frequencymap.get(days);
                    frequencymap.put(days,brefore+1);

                    //计算最大消费金额
                    String totalamountstring = baiJiaInfoinner.getTotalamount();
                    Double totalamout = Double.valueOf(totalamountstring);
                    if(totalamout>maxamount){
                        maxamount = totalamout;
                    }
                    //计算总消费金额
                    sum += totalamout;

                    before = baiJiaInfoinner;
                }
                //平均消费金额
                double avramount = sum/list.size();

                int totaldays = 0;//总天数
                Set<Map.Entry<Integer,Integer>> set = frequencymap.entrySet();
                for(Map.Entry<Integer,Integer> entry :set){
                    Integer frequencydays = entry.getKey();
                    totaldays += frequencydays;
                }
                int avrdays = totaldays/set.size();//平均天数

                //败家指数 = 支付金额平均值*0.3、最大支付金额*0.3、下单频率*0.4
                //支付金额平均值30分（0-20 5 20-60 10 60-100 20 100-150 30 150-200 40 200-250 60 250-350 70 350-450 80 450-600 90 600以上 100  ）
                // 最大支付金额30分（0-20 5 20-60 10 60-200 30 200-500 60 500-700 80 700 100）
                // 下单平率30分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）
                int avraoumtsoce = 0;
                if(avramount>=0 && avramount < 20){
                    avraoumtsoce = 5;
                }else if (avramount>=20 && avramount < 60){
                    avraoumtsoce = 10;
                }else if (avramount>=60 && avramount < 100){
                    avraoumtsoce = 20;
                }else if (avramount>=100 && avramount < 150){
                    avraoumtsoce = 30;
                }else if (avramount>=150 && avramount < 200){
                    avraoumtsoce = 40;
                }else if (avramount>=200 && avramount < 250){
                    avraoumtsoce = 60;
                }else if (avramount>=250 && avramount < 350){
                    avraoumtsoce = 70;
                }else if (avramount>=350 && avramount < 450){
                    avraoumtsoce = 80;
                }else if (avramount>=450 && avramount < 600){
                    avraoumtsoce = 90;
                }else if (avramount>=600){
                    avraoumtsoce = 100;
                }

                int maxaoumtscore = 0;
                if(maxamount>=0 && maxamount < 20){
                    maxaoumtscore = 5;
                }else if (maxamount>=20 && maxamount < 60){
                    maxaoumtscore = 10;
                }else if (maxamount>=60 && maxamount < 200){
                    maxaoumtscore = 30;
                }else if (maxamount>=200 &&maxamount < 500){
                    maxaoumtscore = 60;
                }else if (maxamount>=500 && maxamount < 700){
                    maxaoumtscore = 80;
                }else if (maxamount>=700){
                    maxaoumtscore = 100;
                }

                // 下单平率30分 （0-5 100 5-10 90 10-30 70 30-60 60 60-80 40 80-100 20 100以上的 10）
                int avrdaysscore = 0;
                if(avrdays>=0 && avrdays < 5){
                    avrdaysscore = 100;
                }else if (avramount>=5 && avramount < 10){
                    avrdaysscore = 90;
                }else if (avramount>=10 && avramount < 30){
                    avrdaysscore = 70;
                }else if (avramount>=30 && avramount < 60){
                    avrdaysscore = 60;
                }else if (avramount>=60 && avramount < 80){
                    avrdaysscore = 40;
                }else if (avramount>=80 && avramount < 100){
                    avrdaysscore = 20;
                }else if (avramount>=100){
                    avrdaysscore = 10;
                }
                //计算败家指数
                double totalscore = (avraoumtsoce/100)*30+(maxaoumtscore/100)*30+(avrdaysscore/100)*40;

                String tablename = "userflaginfo";
                String rowkey = userid;
                String famliyname = "baseinfo";
                String colum = "baijiasoce";
                HbaseUtils.putdata(tablename,rowkey,famliyname,colum,totalscore+"");
            }
            env.execute("baijiascore analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
