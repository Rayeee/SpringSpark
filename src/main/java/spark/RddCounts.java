package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by zhugongyi on 2017/4/14.
 */
public class RddCounts {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("/Users/zhugongyi/elemeCode/spark/access.log");

        JavaRDD<String> words = rdd.flatMap(e -> Arrays.asList(e.split(" ")));

        JavaPairRDD<String, Integer> counts = words.mapToPair(e -> new Tuple2<>(e, 1)).reduceByKey((x, y) -> x + y);

        counts.saveAsTextFile("/Users/zhugongyi/elemeCode/spark/rdd_counts");

    }




}
