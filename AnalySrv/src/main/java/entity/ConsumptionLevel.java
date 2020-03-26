package entity;

/**
 *
 */
public class ConsumptionLevel {
    private String consumptiontype;//消费水平 高水平 中等水平 低水平
    private Long count;//数量
    private String groupfield;//分组字段
    private String userid;//用户id
    private String amounttotaol;//金额

    public String getAmounttotaol() {
        return amounttotaol;
    }

    public void setAmounttotaol(String amounttotaol) {
        this.amounttotaol = amounttotaol;
    }

    public String getConsumptiontype() {
        return consumptiontype;
    }

    public void setConsumptiontype(String consumptiontype) {
        this.consumptiontype = consumptiontype;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getGroupfield() {
        return groupfield;
    }

    public void setGroupfield(String groupfield) {
        this.groupfield = groupfield;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }
}
