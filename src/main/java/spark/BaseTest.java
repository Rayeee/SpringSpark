package spark;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zhugongyi on 2017/4/17.
 */
public class BaseTest {

    protected static JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("Rayee"));

    protected static JavaRDD<String> accessRDD = sc.textFile("/Users/zhugongyi/elemeCode/spark/access.log");

    protected static JavaRDD<Integer> intRDD_1 = sc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

    protected static JavaRDD<Integer> intRDD_2 = sc.parallelize(Lists.newArrayList(6, 7, 8, 9, 10, 11, 12, 13, 14, 15));

    protected static JavaRDD<String> stringRDD = sc.parallelize(Lists.newArrayList("hello world", "hi china"));

}
