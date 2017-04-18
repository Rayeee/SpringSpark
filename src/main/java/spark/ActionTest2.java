package spark;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

/**
 * Created by zhugongyi on 2017/4/18.
 */
public class ActionTest2 extends BaseTest {

    public static void main(String[] args) throws InterruptedException {
        System.out.println(stringRDD.reduce((Function2<String, String, String>) (v1, v2) -> v1.concat(v2)));
        System.out.println(intRDD_1.reduce((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2));
        Tuple2<Double, Integer> aggregate = intRDD_1.aggregate(
                new Tuple2<>(0.0, 0),
                (x, y) -> new Tuple2<>(x._1 + y, x._2 + 1),
                (x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2));
        System.out.println(aggregate._1/aggregate._2);
    }
}
