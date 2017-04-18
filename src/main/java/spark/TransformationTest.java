package spark;

import com.eparty.ccp.core.util.JsonUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * Created by zhugongyi on 2017/4/18.
 */
public class TransformationTest extends BaseTest {

    public static void main(String[] args) {
        JavaRDD<Integer> unionRDD = intRDD_1.union(intRDD_2);
        List<Integer> collect = unionRDD.collect();
//        System.out.println(JsonUtils.toJson(collect));
//        System.out.println(unionRDD.count());
        JavaRDD<Integer> distinctRDD = unionRDD.distinct();
//        System.out.println(JsonUtils.toJson(distinctRDD.collect()));
        JavaRDD<Integer> intersectionRDD = intRDD_1.intersection(intRDD_2);
//        System.out.println(intersectionRDD.collect());
        JavaRDD<Integer> subtractRDD = intRDD_1.subtract(intRDD_2);
//        System.out.println(subtractRDD.count() + " " + JsonUtils.toJson(subtractRDD.collect()));
        JavaPairRDD<Integer, Integer> cartesianRDD = intRDD_1.cartesian(intRDD_2);
        System.out.println(cartesianRDD.count() + " " + JsonUtils.toJson(cartesianRDD.collect()));

    }

}
