package reduce;

import entity.YearBase;
import org.apache.flink.api.common.functions.ReduceFunction;

public class YearBaseReduce implements ReduceFunction<YearBase> {
    private static final long serialVersionUID = -4760849061270107911L;

    @Override
    public YearBase reduce(YearBase yearBase, YearBase t1) throws Exception {
        String yeartype = yearBase.getYeartype();
        Long count1 = yearBase.getCount();
        Long count2 = t1.getCount();

        YearBase finalyearBase = new YearBase();
        finalyearBase.setYeartype(yeartype);
        finalyearBase.setCount(count1+count2);
        return finalyearBase;
    }
}
