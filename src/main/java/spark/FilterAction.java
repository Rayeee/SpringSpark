package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zhugongyi on 2017/4/17.
 */
public class FilterAction {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("/Users/zhugongyi/elemeCode/spark/access.log");

        JavaRDD<String> errorRdd = rdd.filter(e -> e.contains("ERROR"));

        System.out.println(errorRdd.first());
    }

}
