package reduce;

import entity.ChaomanAndWomenInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

public class ChaomanwomenfinalReduce implements ReduceFunction<ChaomanAndWomenInfo>{


    @Override
    public ChaomanAndWomenInfo reduce(ChaomanAndWomenInfo chaomanAndWomenInfo1, ChaomanAndWomenInfo chaomanAndWomenInfo2) throws Exception {

        String chaotype = chaomanAndWomenInfo1.getChaotype();

        long count1 = chaomanAndWomenInfo1.getCount();

        long count2 = chaomanAndWomenInfo2.getCount();

        ChaomanAndWomenInfo finalchao = new ChaomanAndWomenInfo();
        finalchao.setChaotype(chaotype);
        finalchao.setCount(count1+count2);


        return finalchao;
    }
}
