package spark;

import com.eparty.ccp.core.util.JsonUtils;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * Created by zhugongyi on 2017/4/17.
 */
public class ActionTest1 extends BaseTest {

    public static void main(String... args) {
        JavaRDD<String> errorRDD = accessRDD.filter(e -> e.contains("ERROR"));
        System.out.println("error lines count = " + errorRDD.count());
        for(String line : errorRDD.take(10)){
            System.out.println(line);
        }
        //获取整 个 RDD 中的数据
        List<String> collect = accessRDD.collect();
        System.out.println(accessRDD.count());
        System.out.println(collect.size());
//        System.out.println(JsonUtils.toJson(collect));
        errorRDD.saveAsTextFile("/Users/zhugongyi/elemeCode/spark/rdd_output");
    }
}
