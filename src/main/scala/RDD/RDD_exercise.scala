package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_exercise {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("案例实操")
        val sc = new SparkContext(conf)

        //TODO 案例实操：统计出每一个省份每个广告被点击数量排行的 Top3

        //      1. 获取原始数据：时间戳 省份 城市 用户 广告
        val dataRDD = sc.textFile("datas/agent.log")

        //      2. 将原始数据进行结构转换
        //         => ((省份,广告),1)
        val rdd1 = dataRDD.map(
            line => {
                val data = line.split(" ")
                ((data(1), data(4)), 1)
            }
        )
        //      3. 将转换过后的数据进行分组聚合
        //         ((省份, 广告), 1)
        //         => ((省份, 广告), sum)
        val rdd2 = rdd1.reduceByKey(_ + _)
        //      4. 将聚合结果进行结构转换
        //         ((省份, 广告), sum)
        //         => (省份, (广告, sum))
        val rdd3 = rdd2.map(a => (a._1._1, (a._1._2, a._2)))
        //      5. 将转换过后的数据按省份进行分组
        //         (省份, [(广告A, sumA), (广告B, sumB), (广告C, sumC)])
        val rdd4 = rdd3.groupByKey()
        //      6. 将分组后的数据组内按sum降序排序，取前三名
        val res = rdd4.mapValues(
            iter => {
                //这里toList后就变成Scala的List了,Scala中List的SortBy倒序后面要加(Ordering.Int.reverse)
                //.take(3)取前三名
                iter.toList.sortBy(list => list._2)(Ordering.Int.reverse).take(3)
            }
        )
        //      7. 采集数据，控制台打印
        res.collect().foreach(println)

        sc.stop()
    }
}
