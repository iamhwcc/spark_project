package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable

object RDD_Transform {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_transform")
        val sc = new SparkContext(conf)

        //        val dataRDD = sc.makeRDD(List(List(1, 2), 3, List(3, 4)))
        //        val newRDD = dataRDD.flatMap {
        //            case list: List[_] => list
        //            case num => List(num)
        //        }
        //        newRDD.collect().foreach(println)

        //        val dataRDD = sc.makeRDD(List(1, 2, 3, 4), 2)
        //        val dataRDD1: RDD[Array[Int]] = dataRDD.glom()
        //        dataRDD1.collect().foreach(data => println(data.mkString(",")))
        //
        //        groupBy
        //        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
        //        val rdd1 = rdd.groupBy(_ % 2)
        //        rdd1.collect().foreach(println)
        //
        //        val rdd = sc.makeRDD(List(1, 3, 5, 4, 2), 2)
        //        val sortRDD = rdd.sortBy(num => num)
        //        sortRDD.collect().foreach(println)
        //
        //
        //        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 5)))
        //        rdd.groupByKey().foreach(println)
        //
        //        aggregateByKey
        //        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)
        //        //分区内取最大值，分区间求和
        //        //存在函数柯里化，有两个参数列表
        //        //第一个参数列表是初始值，用于碰见第一个Key的时候，和Value进行分区内计算
        //        //第二个参数列表，其中第一个参数是分区内计算规则，第二个参数是分区间计算规则
        //        rdd.aggregateByKey(Int.MinValue)(
        //            (x, y) => math.max(x, y),
        //            (x, y) => x + y
        //        ).collect().foreach(println)


        //        //获取相同的Key的Value的平均值
        //        // (a,3),(b,4)
        //        val rdd = sc.makeRDD(List(
        //            ("a", 1), ("a", 2), ("b", 3),
        //            ("b", 4), ("b", 5), ("a", 6)
        //        ), 2)
        //        val rdd1 = rdd.aggregateByKey((0, 0))( //第一个0是用于相加的初始值，第二个是出现次数的初始值
        //            (tuple, v) => {
        //                (tuple._1 + v, tuple._2 + 1)
        //            },
        //            (t1, t2) => {
        //                (t1._1 + t2._1, t1._2 + t2._2)
        //            }
        //        )
        //        val res = rdd1.mapValues {
        //            case (num, cnt) => {
        //                num / cnt
        //            }
        //        }
        //        res.collect().foreach(println)

        //        val rdd = sc.makeRDD(List(
        //            ("a", 1), ("a", 2), ("b", 3),
        //            ("b", 4), ("b", 5), ("a", 6)
        //        ), 2)

        //combineByKey方法需要三个参数
        //第一个参数表示：将相同的Key的第一个数据进行结构的转换
        //第二个参数表示：分区内计算规则
        //第三个参数表示：分区间计算规则
        //        val newRDD = rdd.combineByKey(v => (v, 1),//结构转换,后续的tuple需要指明类型
        //            (tuple: (Int, Int), v) => {
        //                (tuple._1 + v, tuple._2 + 1)
        //            },
        //            (t1: (Int, Int), t2: (Int, Int)) => {
        //                (t1._1 + t2._1, t1._2 + t2._2)
        //            }
        //        )

        //求每个 key 的平均值
        //        val rdd = sc.makeRDD(List(
        //            ("a", 1), ("a", 2), ("b", 3),
        //            ("b", 4), ("b", 5), ("a", 6)
        //        ))
        //
        //        val newRDD = rdd.combineByKey((v => (v, 1)),
        //            (tuple: (Int, Int), value) => {
        //                (tuple._1 + value, tuple._2 + 1)
        //            },
        //            (t1: (Int, Int), t2: (Int, Int)) => {
        //                (t1._1 + t2._1, t2._2 + t1._2)
        //            }
        //        )
        //
        //        val res = newRDD.mapValues(a => a._1 / a._2)
        //        res.collect().foreach(println)


//        val dataRDD1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
//        val dataRDD2 = sc.makeRDD(List(("a", 1), ("b", 2)))
//        val rdd = dataRDD1.leftOuterJoin(dataRDD2)
//        println(rdd.collect().mkString(","))


        sc.stop()

    }
}
