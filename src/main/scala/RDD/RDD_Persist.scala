package RDD

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Persist {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Persist")
        val sc = new SparkContext(conf)

        val list = List("Hello Scala","Hello Spark","Hello Flink")
        val rdd = sc.makeRDD(list)
        val rdd1 = rdd.flatMap(_.split(" "))
        val rdd2 = rdd1.map((_,1))
        //rdd2.cache()//持久化
        rdd2.persist(StorageLevel.DISK_ONLY)//持久化到磁盘
        val res1 = rdd2.reduceByKey(_ + _)
        res1.collect().foreach(println)
        println("************************")
        val res2 = rdd2.groupByKey()
        res2.collect().foreach(println)
        sc.stop()
    }

}
