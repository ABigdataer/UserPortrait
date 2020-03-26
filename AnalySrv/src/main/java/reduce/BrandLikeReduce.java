package reduce;

import entity.BrandLike;
import org.apache.flink.api.common.functions.ReduceFunction;


public class BrandLikeReduce implements ReduceFunction<BrandLike> {
    @Override
    public BrandLike reduce(BrandLike brandLike, BrandLike t1) throws Exception {
        String brand = brandLike.getBrand();
        long count1 = brandLike.getCount();
        long count2 = t1.getCount();
        BrandLike brandLikefinal = new BrandLike();
        brandLikefinal.setBrand(brand);
        brandLikefinal.setCount(count1+count2);
        return brandLikefinal;
    }
}
