package SparkCore_Final_Exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io

object Require01_2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Require01_2").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val text = sc.textFile("datas/user_visit_action.txt")

        //进行数据结构的转换
        // 点击：(品类，(1,0,0))
        // 下单：(品类，(0,1,0))
        // 支付：(品类，(0,0,1))
        val flatRDD = text.flatMap(
            action => {
                val data = action.split("_")
                if (data(6) != "-1") {
                    List((data(6), (1, 0, 0)))
                } else if (data(8) != "null") {
                    val orderData = data(8).split(",")
                    orderData.map(id => {
                        (id, (0, 1, 0))
                    })
                } else if (data(10) != "null") {
                    val payData = data(10).split(",")
                    payData.map(id => {
                        (id, (0, 0, 1))
                    })
                } else {
                    //其他情况没数据，返回空集合
                    Nil
                }
            })
        //此时flatRDD就是很多很多[品类，(1,0,0)][品类，(0,1,0)][品类，(0,0,1)]
        //然后reduceByKey根据相同的Key聚合就会形成[品类，(点击总数,下单总数,支付总数)]
        val Tuple = flatRDD.reduceByKey((t1, t2) => {
            (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
        })
        Tuple.sortBy(_._2, false).take(10).foreach(println)

        sc.stop()
    }
}
