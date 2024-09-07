package SparkCore_Final_Exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Require02 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Require02").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val text = sc.textFile("datas/user_visit_action.txt")
        text.cache()
        //TODO:在需求一的基础上，增加每个品类用户session的点击统计
        val top10IDs = Top10Result(text) //Top10的cid
        //过滤掉不是前十的点击数据
        val clickData = text.filter(action => {
            val data = action.split("_")
            if (data(6) != "-1") {
                top10IDs.contains(data(6))
            } else {
                false
            }
        })

        val reduceRDD = clickData.map(action => {
            val data = action.split("_")
            ((data(6), data(2)), 1) //((品类,Session),1)
        }).reduceByKey(_ + _)

        //结构转换  ((品类,Session),sum) => (品类,(Session,sum))
        val newRDD: RDD[(String, (String, Int))] = reduceRDD.map(element => {
            (element._1._1, (element._1._2, element._2))
        })

        val group: RDD[(String, Iterable[(String, Int)])] = newRDD.groupByKey()

        //将分组后的数据进行点击量排序，取前10
        val res: RDD[(String, List[(String, Int)])] = group.mapValues(iter => {
            iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)//这里是scala的take方法，所以最后还要collect
        })

        res.collect().foreach(println)
        sc.stop()
    }

    def Top10Result(text: RDD[String]) = {
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

        val Tuple = flatRDD.reduceByKey((t1, t2) => {
            (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
        })
        Tuple.sortBy(_._2, ascending = false).take(10).map(_._1) //获取cid
    }
}
