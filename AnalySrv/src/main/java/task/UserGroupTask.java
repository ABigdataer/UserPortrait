package task;

import entity.UserGroupInfo;
import kmeans.*;
import map.KMeansFinalusergroupMap;
import map.UserGroupMap;
import map.UserGroupMapbyreduce;
import reduce.UserGroupInfoReduce;
import reduce.UserGroupbykmeansReduce;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import java.util.*;

/**
 * 使用K-Means算法实现用户分群
 */
public class UserGroupTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        //返回所有用户的历史订单数据
        DataSet<UserGroupInfo> mapresult = text.map(new UserGroupMap());
        //根据用户ID进行分组，然后将每个用户的数据聚合到一个List内
        DataSet<UserGroupInfo> reduceresutl = mapresult.groupBy("groupfield").reduce(new UserGroupInfoReduce());
        //计算每个用户的平均消费金额、消费最大金额、消费频次、消费类目1数量、消费类目2数量、消费类目3数量、消费时间点1数量、消费时间点2数量、消费时间点3数量、消费时间点4数量
        DataSet<UserGroupInfo> mapbyreduceresult = reduceresutl.map(new UserGroupMapbyreduce());
        //对所有用户数据进行随机分组，并用K-Means算法进行预测分析，返回预测结果
        DataSet<ArrayList<Point>> finalresult =  mapbyreduceresult.groupBy("groupfield").reduceGroup(new UserGroupbykmeansReduce());

        try {
            List<ArrayList<Point>> reusltlist = finalresult.collect();
            ArrayList<float[]> dataSet = new ArrayList<float[]>();
            for(ArrayList<Point> array:reusltlist){
                for(Point point:array){
                    //获取每个中心类所属的节点数据
                    dataSet.add(point.getlocalArray());
                }
            }
            KMeansRunbyusergroup kMeansRunbyusergroup =new KMeansRunbyusergroup(6, dataSet);

            Set<Cluster> clusterSet = kMeansRunbyusergroup.run();
            List<Point> finalClutercenter = new ArrayList<Point>();
            int count= 100;
            for(Cluster cluster:clusterSet){
                Point point = cluster.getCenter();
                point.setId(count++);
                finalClutercenter.add(point);
            }
            DataSet<Point> flinalMap = mapbyreduceresult.map(new KMeansFinalusergroupMap(finalClutercenter));
            env.execute("UserGroupTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
