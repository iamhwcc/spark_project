package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Partition {
    def main(args: Array[String]): Unit = {
        //准备Spark环境
        val conf = new SparkConf().setAppName("RDD")
        val sc = new SparkContext(conf)

        //RDD并行度 & 分区
        //makeRDD参数  参数1:集合 参数2:分区数量
        //参数2可有可无，若没有，则默认使用当前运行环境的核数
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
        rdd.saveAsTextFile("hdfs://192.168.142.100:9000/outPutPar")
    }

}
