package map;

import entity.YearBase;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import util.DateUtils;
import util.HbaseUtils;

public class YearBaseMap implements MapFunction<String, YearBase> {
    private static final long serialVersionUID = 6361216340981340943L;

    @Override
    public YearBase map(String s) throws Exception {
        if (StringUtils.isBlank(s))
        {
            return null;
        }
        String[] userInfos = s.split(",");
        String userid = userInfos[0];
        String username = userInfos[1];
        String sex = userInfos[2];
        String telphone = userInfos[3];
        String email = userInfos[4];
        String age = userInfos[5];
        String registerTime = userInfos[6];
        String usetype = userInfos[7]; //终端类型

        String yearbasetype = DateUtils.getYearbasebyAge(age);
        String tablename = "userflaginfo";
        String rowkey = userid;
        String famliyname = "baseinfo";
        String colum = "yearbase";//年代
        //往HBase里面插入用户对应的年代标签数据
        HbaseUtils.putdata(tablename,rowkey,famliyname,colum,yearbasetype);

        YearBase yearBase = new YearBase();
        String groupfeild = "yearbase=="+yearbasetype;
        yearBase.setYeartype(yearbasetype);
        yearBase.setCount(1L);
        yearBase.setGroupfield(groupfeild);

        return yearBase;
    }
}
