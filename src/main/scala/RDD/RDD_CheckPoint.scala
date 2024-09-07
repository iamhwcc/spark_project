package RDD

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RDD_CheckPoint {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Persist")
        val sc = new SparkContext(conf)


        val list = List("Hello Scala","Hello Spark","Hello Flink")
        val rdd = sc.makeRDD(list)
        val rdd1 = rdd.flatMap(_.split(" "))
        val rdd2 = rdd1.map((_,1))

        //checkpoint需要落盘，需要指定检查点保存路径，一般保存在HDFS
        //程序结束后不会删除
        sc.setCheckpointDir("myCheckPoint")
        rdd2.checkpoint()

        val res1 = rdd2.reduceByKey(_ + _)
        res1.collect().foreach(println)
        println("************************")
        val res2 = rdd2.groupByKey()
        res2.collect().foreach(println)
        sc.stop()
    }

}
