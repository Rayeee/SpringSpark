package spark;

import org.apache.spark.api.java.JavaRDD;

/**
 * Created by zhugongyi on 2017/4/17.
 */
public class MapFilter extends BaseTest {

    public static void main(String[] args) {
        JavaRDD<String> rdd1 = accessRDD.filter(e -> e.contains("ERROR"));
        JavaRDD<Boolean> error = accessRDD.map(e -> e.contains("ERROR"));
//        System.out.println(error.count());
//        System.out.println(rdd1.count());
//        System.out.println(accessRDD.count());
        error.foreach(e -> {
            System.out.println(e.booleanValue());
        });
    }

}
