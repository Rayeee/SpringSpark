package spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by zhugongyi on 2017/4/17.
 */
public class MapTest extends BaseTest {

    public static void main(String[] args) {
        JavaRDD<Integer> result = intRDD_1.map(e -> e * e);
        System.out.println(StringUtils.join(result.collect(), ","));
    }

}
