package SparkCore_Final_Exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Require03 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Require03").setMaster("local[*]")
        val sc = new SparkContext(conf)

        //TODO：页面单跳转换率统计
        val text: RDD[String] = sc.textFile("datas/user_visit_action.txt")

        val actionDataRDD: RDD[UserVisitActions] = text.map(action => {
            val data: Array[String] = action.split("_")
            //将每个数据放到UserVisitActions里,方便取
            UserVisitActions(
                data(0),
                data(1).toLong,
                data(2),
                data(3).toLong,
                data(4),
                data(5),
                data(6).toLong,
                data(7).toLong,
                data(8),
                data(9),
                data(10),
                data(11),
                data(12).toLong,
            )
        })
        actionDataRDD.cache()

        //TODO:性能优化，对指定的页面跳转统计单跳转换率
        //指定的页面ID
        val myPageID: List[Long] = List(1, 2, 3, 4, 5, 6, 7)
        // 指定跳转顺序   1-2 2-3 3-4 4-5 5-6 6-7
        val myPageChange: List[(Long, Long)] = myPageID.zip(myPageID.tail)


        //TODO:计算分母
        val idTuple: RDD[(Long, Long)] = actionDataRDD.filter(action => {
            //只保留指定的跳转顺序的前一个页面，不包括7，用init
            myPageID.init.contains(action.page_id)
        }).map(action => {
            (action.page_id, 1L)
        })
        val pageIDCount: RDD[(Long, Long)] = idTuple.reduceByKey(_ + _)

        val pageIDCountMap: Map[Long, Long] = pageIDCount.collect().toMap //转换为Map方便后续分子去找分母中的值

        //TODO:计算分子
        //根据Session分组
        val sessionGroup: RDD[(String, Iterable[UserVisitActions])] = actionDataRDD.groupBy(_.session_id)

        val tupleRDD: RDD[(String, List[((Long, Long), Int)])] = sessionGroup.mapValues(iter => {
            val sortedList: List[UserVisitActions] = iter.toList.sortBy(_.action_time)
            val pageIdList: List[Long] = sortedList.map(_.page_id)
            //页面访问顺序 1 -> 2 -> 3 -> 4
            //[1,2,3,4] => [((1,2),1),((2,3),1),((3,4),1)]
            //使用拉链
            //[1,2,3,4]
            //[2,3,4]
            val pageVisitOrder: List[(Long, Long)] = pageIdList.zip(pageIdList.tail)

            //将不合法的跳转顺序排除
            pageVisitOrder.filter(t => {
                myPageChange.contains(t)
            }).map(t => {
                (t, 1)
            })
        })
        val flatRDD: RDD[((Long, Long), Int)] = tupleRDD.map(_._2).flatMap(list => list)
        //((1,2),sum)
        val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)

        //TODO:计算单挑转化率
        //分子 / 分母
        dataRDD.foreach {
            //根据pageID1找到分母中的数据
            case ((pageID1, pageID2), sum) => {
                val denominator: Long = pageIDCountMap.getOrElse(pageID1, 0L) //分母
                println(s"页面(${pageID1}-->${pageID2})的单跳转换率为：" + "%.2f".format((sum.toDouble / denominator) * 100) + "%")
            }
        }
        sc.stop()
    }

    //用户访问动作表
    private case class UserVisitActions(date: String, //用户点击行为的日期
                                        user_id: Long, //用户的 ID
                                        session_id: String, //Session 的 ID
                                        page_id: Long, //某个页面的 ID
                                        action_time: String, //动作的时间点
                                        search_keyword: String, //用户搜索的关键词
                                        click_category_id: Long, //某一个商品品类的 ID
                                        click_product_id: Long, //某一个商品的 ID
                                        order_category_ids: String, //一次订单中所有品类的 ID 集合
                                        order_product_ids: String, //一次订单中所有商品的 ID 集合
                                        pay_category_ids: String, //一次支付中所有品类的 ID 集合
                                        pay_product_ids: String, //一次支付中所有商品的 ID 集合
                                        city_id: Long //城市 id
                                       )
}
