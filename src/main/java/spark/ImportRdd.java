package spark;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zhugongyi on 2017/4/17.
 */
public class ImportRdd {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Rayee");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd1 = sc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5));

        rdd1.foreach(e -> System.out.println());

        JavaRDD<Integer> rdd2 = sc.parallelize(Lists.newArrayList(6, 7));

        JavaRDD<Integer> rdd3 = rdd1.union(rdd2);

        System.out.println(rdd3.count());
    }

}
