package map;

import entity.CarrierInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import util.CarrierUtils;
import util.HbaseUtils;

public class CarrierMap implements MapFunction<String, CarrierInfo>
{
    private static final long serialVersionUID = 7800053287826281148L;

    @Override
    public CarrierInfo map(String s) throws Exception {

        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] userinfos = s.split(",");
        String userid = userinfos[0];
        String username = userinfos[1];
        String sex = userinfos[2];
        String telphone = userinfos[3];
        String email = userinfos[4];
        String age = userinfos[5];
        String registerTime = userinfos[6];
        String usetype = userinfos[7];//'终端类型：0、pc端；1、移动端；2、小程序端'

        int carriertype = CarrierUtils.getCarrierByTel(telphone);//获取运营商类型代号
        String carriertypestring = carriertype==0?"未知运营商":carriertype==1?"移动用户":carriertype==2?"联通用户":"电信用户";

        String tablename = "userflaginfo";
        String rowkey = userid;
        String famliyname = "baseinfo";
        String colum = "carrierinfo";//运营商
        HbaseUtils.putdata(tablename,rowkey,famliyname,colum,carriertypestring);
        CarrierInfo carrierInfo = new CarrierInfo();
        String groupfield = "carrierInfo=="+carriertype;
        carrierInfo.setCount(1l);
        carrierInfo.setCarrier(carriertypestring);
        carrierInfo.setGroupfield(groupfield);
        return carrierInfo;
    }
}
