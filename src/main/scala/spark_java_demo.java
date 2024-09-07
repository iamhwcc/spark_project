import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author stalwarthuang
 * @since 2024-07-30 星期二 09:14:31
 */
public class spark_java_demo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("spark_java_demo");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("/Users/hwc/Documents/Spark Project/Spark_WordCount/datas/word.txt");

        // java的flatMap返回的是一个迭代器，java如果不清楚返回结果就不要写lambda表达式，new出函数重写方法更好
        JavaRDD<String> rdd1 = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        rdd1.collect().forEach(System.out::println);
    }
}
