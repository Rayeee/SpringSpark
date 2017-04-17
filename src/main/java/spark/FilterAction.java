package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Created by zhugongyi on 2017/4/17.
 */
public class FilterAction {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("/Users/zhugongyi/elemeCode/spark/access.log");

        JavaRDD<String> errorRdd = rdd.filter(e -> e.contains("ERROR"));

        //缓存rdd
        errorRdd.persist(StorageLevel.MEMORY_ONLY());

        long start = System.currentTimeMillis();

        for (int i = 0; i < 100; i++) {
            errorRdd.first();
            errorRdd.count();
        }
        System.out.println(System.currentTimeMillis() - start);
    }

}
