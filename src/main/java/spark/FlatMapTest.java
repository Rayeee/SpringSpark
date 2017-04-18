package spark;

import com.eparty.ccp.core.util.JsonUtils;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;

/**
 * Created by zhugongyi on 2017/4/17.
 */
public class FlatMapTest extends BaseTest {

    public static void main(String[] args) {
        JavaRDD<String[]> mapRes = stringRDD.map(e -> e.split(" "));
        System.out.println(JsonUtils.toJson(mapRes.first()));
        JavaRDD<Object> result = stringRDD.flatMap(s -> Lists.newArrayList(s.split(" ")));
        System.out.println(result.count());
        System.out.println(result.first());
    }

}
