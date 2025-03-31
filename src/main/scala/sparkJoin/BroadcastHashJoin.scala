package sparkJoin

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BroadcastHashJoin {
    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = SparkSession.builder().master("local[*]")
            .appName("BigRDD Join SmallRDD").getOrCreate()
        val sc: SparkContext = sparkSession.sparkContext
        val list1: List[(String, Int)] = List(("jame", 23), ("wade", 3), ("kobe", 24))
        val list2: List[(String, Int)] = List(("jame", 13), ("wade", 6), ("kobe", 16))
        val bigRDD: RDD[(String, Int)] = sc.makeRDD(list1)
        val smallRDD: RDD[(String, Int)] = sc.makeRDD(list2)

        val small_rdd_inmemory: Broadcast[Array[(String, Int)]] = sc.broadcast(smallRDD.collect())
        val joinedRDD: RDD[(String, (Int, Int))] = bigRDD.mapPartitions(partition => {
            val small_table: Array[(String, Int)] = small_rdd_inmemory.value
            partition.map(e => {
                joinUtil(e, small_table)
            })
        })
        joinedRDD.foreach(x => println(x));
    }

    def joinUtil(BigRdd: (String, Int), smallRdd: Array[(String, Int)]): (String, (Int, Int)) = {
        var res: (String, (Int, Int)) = null;
        smallRdd.foreach(x => {
            if (x._1 == BigRdd._1) {
                res = (BigRdd._1, (BigRdd._2, x._2))
            }
        })
        res
    }
}
