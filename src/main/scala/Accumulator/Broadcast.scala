package Accumulator

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Broadcast {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Broadcast")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD(List(
            ("a",1),("b",2),("c",3)
        ))

        val map = mutable.Map(
            ("a", 4), ("b", 5), ("c", 6)
        )

        //广播变量
        val bc = sc.broadcast(map)

        rdd.map{
            case (w,c) => {
                val l = bc.value.getOrElse(w,0)
                (w,(c,l))
            }
        }.collect().foreach(println)

        sc.stop()
    }

}
