import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author stalwarthuang
 * @since 2024-07-30 星期二 09:14:31
 */
public class read_json {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("spark_java_demo");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("/Users/hwc/Documents/Spark Project/Spark_WordCount/datas/user.txt");
        JavaRDD<Integer> age = rdd.map(new Function<String, Integer>() {

            @Override
            public Integer call(String s) throws Exception {
                return JSONObject.parseObject(s).getInteger("age");
            }
        });
        age.collect().forEach(System.out::println);
    }
}
