package SparkCore_Final_Exercise

import org.apache.spark.{SparkConf, SparkContext}

object Require01 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Require01").setMaster("local[*]")
        val sc = new SparkContext(conf)

        //TODO: Top 10
        // 先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
        // 思路：(品类，点击总数) (品类，下单总数) (品类，支付总数)
        // 排序思路可模仿元组的排序(先比较第一个，再比较第二个)，构造成[品类，(点击总数,下单总数,支付总数)]

        val text = sc.textFile("datas/user_visit_action.txt")
        //统计点击
        val clickRDD1 = text.filter(action => {
            val data = action.split("_")
            data(6) != "-1" //排除
        })
        val clickRDD2 = clickRDD1.map(line => {
            val data = line.split("_")
            (data(6), 1)
        })
        val clickRes = clickRDD2.reduceByKey(_ + _)
        //统计下单
        val orderRDD1 = text.filter(action => {
            val data = action.split("_")
            data(8) != "null" //排除
        })
        //这里如果用map返回的是一个(String,Int)的数组，所以我们要打散才能reduceByKey
        val orderRes = orderRDD1.flatMap(action => {
            val data = action.split("_")
            val ids = data(8)
            val id = ids.split(",")
            id.map(ID => (ID, 1))
        }).reduceByKey(_ + _)
        //统计支付
        val payRDD1 = text.filter(action => {
            val data = action.split("_")
            data(10) != "null" //排除
        })
        val payRes = payRDD1.flatMap(action => {
            val data = action.split("_")
            val ids = data(10)
            val id = ids.split(",")
            id.map(ID => (ID, 1))
        }).reduceByKey(_ + _)

        //对于一个Key，假设其中有一个没有值，cogroup都会补齐
        val group = clickRes.cogroup(orderRes, payRes) //此时迭代器中只有一个元素或空
        val tuple = group.mapValues((iter) => { //iter表示三个迭代器，分别是点击总数迭代器，下单总数迭代器，支付总数迭代器
            var clickCnt = 0
            val clickIter = iter._1.iterator
            if (clickIter.hasNext) { //判断迭代器中是否有数字
                clickCnt = clickIter.next() //如果有，放入clickCnt就行，因为迭代器只有一个元素
            }
            var orderCnt = 0
            val orderIter = iter._2.iterator
            if (orderIter.hasNext) {
                orderCnt = orderIter.next()
            }
            var payCnt = 0
            val payIter = iter._3.iterator
            if (payIter.hasNext) {
                payCnt = payIter.next()
            }
            (clickCnt, orderCnt, payCnt)
        })
        tuple.sortBy(_._2, ascending = false).take(10).foreach(println)
        sc.stop()
    }
}
