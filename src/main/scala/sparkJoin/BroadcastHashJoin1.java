package sparkJoin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class BroadcastHashJoin1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastJoinExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // 准备数据
            List<Tuple2<String, Integer>> list1 = Arrays.asList(
                    new Tuple2<>("jame", 23),
                    new Tuple2<>("wade", 3),
                    new Tuple2<>("kobe", 24)
            );

            List<Tuple2<String, Integer>> list2 = Arrays.asList(
                    new Tuple2<>("jame", 13),
                    new Tuple2<>("wade", 6),
                    new Tuple2<>("kobe", 16)
            );

            JavaRDD<Tuple2<String, Integer>> bigRDD = sc.parallelize(list1);
            JavaRDD<Tuple2<String, Integer>> smallRDD = sc.parallelize(list2);

            // 使用方式1：调用 broadcastHashJoin
            JavaRDD<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> result1 =
                    broadcastHashJoin(bigRDD, smallRDD);

            System.out.println("===== 使用 broadcastHashJoin 的结果 =====");
            for (Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> pair : result1.collect()) {
                System.out.printf("连接结果: (%s, %d) - (%s, %d)%n",
                        pair._1._1, pair._1._2,
                        pair._2._1, pair._2._2);
            }
        } finally {
            sc.stop();
        }
    }

    public static JavaRDD<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> broadcastHashJoin(
            JavaRDD<Tuple2<String, Integer>> left,
            JavaRDD<Tuple2<String, Integer>> right) {

        Broadcast<Map<String, List<Tuple2<String, Integer>>>> rightHashMap = buildBroadcastHashMap(right, JavaSparkContext.fromSparkContext(left.context()));
        return performJoin(left, rightHashMap);
    }

    public static Broadcast<Map<String, List<Tuple2<String, Integer>>>> buildBroadcastHashMap(
            JavaRDD<Tuple2<String, Integer>> rdd,
            JavaSparkContext sc) {

        Map<String, List<Tuple2<String, Integer>>> groupedMap = new HashMap<>();

        for (Tuple2<String, Integer> record : rdd.collect()) {
            groupedMap.computeIfAbsent(record._1, k -> new ArrayList<>()).add(record);
        }

        return sc.broadcast(groupedMap);
    }

    // 辅助方法2：执行连接操作
    public static JavaRDD<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> performJoin(
            JavaRDD<Tuple2<String, Integer>> left,
            Broadcast<Map<String, List<Tuple2<String, Integer>>>> broadcastMap) {

        return left.flatMap(pair -> {
            String key = pair._1;
            List<Tuple2<String, Integer>> matches = broadcastMap.value().getOrDefault(key, Collections.emptyList());

            List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> joined = new ArrayList<>();
            for (Tuple2<String, Integer> match : matches) {
                joined.add(new Tuple2<>(pair, match));
            }

            return joined.iterator();
        });
    }
}
