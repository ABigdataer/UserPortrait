package log;


public class CollectProductLog {
    private int productid;//商品id
    private int producttypeid;//商品类别id
    private String opertortime;//操作时间
    private int opertortype;//操作类型，0、收藏，1、取消
    private int userid;//用户id
    private int usetype;//终端类型：0、pc端；1、移动端；2、小程序端'
    private String ip;// 用户ip

    private String brand;//品牌

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public int getProductid() {
        return productid;
    }

    public void setProductid(int productid) {
        this.productid = productid;
    }

    public int getProducttypeid() {
        return producttypeid;
    }

    public void setProducttypeid(int producttypeid) {
        this.producttypeid = producttypeid;
    }

    public String getOpertortime() {
        return opertortime;
    }

    public void setOpertortime(String opertortime) {
        this.opertortime = opertortime;
    }

    public int getOpertortype() {
        return opertortype;
    }

    public void setOpertortype(int opertortype) {
        this.opertortype = opertortype;
    }

    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public int getUsetype() {
        return usetype;
    }

    public void setUsetype(int usetype) {
        this.usetype = usetype;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
