package reduce;

import entity.KeyWordEntity;
import org.apache.flink.api.common.functions.ReduceFunction;


public class KeyWordReduce2 implements ReduceFunction<KeyWordEntity>{


    @Override
    public KeyWordEntity reduce(KeyWordEntity keyWordEntity1, KeyWordEntity keyWordEntity2) throws Exception {

        long count1 = keyWordEntity1.getTotaldocumet()==null?1l:keyWordEntity1.getTotaldocumet();
        long count2 = keyWordEntity2.getTotaldocumet()==null?1l:keyWordEntity2.getTotaldocumet();
        KeyWordEntity keyWordEntityfinal = new KeyWordEntity();
        keyWordEntityfinal.setTotaldocumet(count1 + count2);
        return keyWordEntityfinal;
    }
}
