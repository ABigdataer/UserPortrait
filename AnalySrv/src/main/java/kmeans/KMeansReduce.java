package kmeans;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

public class KMeansReduce implements GroupReduceFunction<KMeans,ArrayList<Point>> {

    @Override
    public void reduce(Iterable<KMeans> iterable, Collector<ArrayList<Point>> collector) throws Exception {

        Iterator<KMeans> iterator = iterable.iterator();
        ArrayList<float[]> dataSet = new ArrayList<float[]>();
        while(iterator.hasNext()){
            KMeans kMeans = iterator.next();
            float[] f = new float[]{Float.valueOf(kMeans.getVariable1()),Float.valueOf(kMeans.getVariable2()),Float.valueOf(kMeans.getVariable3())};
            dataSet.add(f);
        }

        KMeansRun kRun =new KMeansRun(6, dataSet);
        //对测试数据集进行聚类分析运算
        Set<Cluster> clusterSet = kRun.run();

        ArrayList<Point> arrayList = new ArrayList<Point>();
        for(Cluster cluster:clusterSet)
        {
            arrayList.add(cluster.getCenter());
        }
        collector.collect(arrayList);
    }
}
