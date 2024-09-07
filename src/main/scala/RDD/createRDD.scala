package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object createRDD {
    def main(args: Array[String]): Unit = {
        //准备Spark环境
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(conf)

        val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
        val dataRDD2 = sc.makeRDD(List(("a",1),("b",2)))
        val rdd = dataRDD1.leftOuterJoin(dataRDD2)
        rdd.collect().foreach(println)
    }

}
