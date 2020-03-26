package reduce;

import entity.CarrierInfo;
import org.apache.flink.api.common.functions.ReduceFunction;


public class CarrierReduce implements ReduceFunction<CarrierInfo>{

    @Override
    public CarrierInfo reduce(CarrierInfo carrierInfo, CarrierInfo t1) throws Exception {

        String carrier = carrierInfo.getCarrier();
        Long count1 = carrierInfo.getCount();
        Long count2 = t1.getCount();

        CarrierInfo carrierInfofinal = new CarrierInfo();
        carrierInfofinal.setCarrier(carrier);
        carrierInfofinal.setCount(count1+count2);

        return carrierInfofinal;
    }
}
