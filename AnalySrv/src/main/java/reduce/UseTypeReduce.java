package reduce;

import entity.UseTypeInfo;
import org.apache.flink.api.common.functions.ReduceFunction;


public class UseTypeReduce implements ReduceFunction<UseTypeInfo> {

    @Override
    public UseTypeInfo reduce(UseTypeInfo useTypeInfo, UseTypeInfo t1) throws Exception {
        String usertype = useTypeInfo.getUsetype();
        Long count1 = useTypeInfo.getCount();

        Long count2 = t1.getCount();

        UseTypeInfo useTypeInfofinal = new UseTypeInfo();
        useTypeInfofinal.setUsetype(usertype);
        useTypeInfofinal.setCount(count1+count2);
        return useTypeInfofinal;
    }
}
