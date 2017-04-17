package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zhugongyi on 2017/4/17.
 */
public class BaseTest {

    protected static JavaRDD<String> accessRDD =
            new JavaSparkContext(new SparkConf().
                    setMaster("local").
                    setAppName("Rayee"))
            .textFile("/Users/zhugongyi/elemeCode/spark/access.log");
}
