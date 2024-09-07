package SparkCore_Final_Exercise

import org.apache.spark.{SparkConf, SparkContext}

object Require01_1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Require01_1").setMaster("local[*]")
        val sc = new SparkContext(conf)

        // TODO: Top 10 方法2
        //  一次性统计出每个品类的点击总数、下单总数、支付总数，优化性能
        //  [品类，(点击总数,下单总数,支付总数)]

        val text = sc.textFile("datas/user_visit_action.txt")
        text.cache()
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
        }).reduceByKey(_+_)
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

        //(品类，点击总数) => [品类,(点击总数,0,0)]
        //(品类，下单总数) => [品类,(0,下单总数,0)]      两两聚合
        //(品类，支付总数) => [品类,(0,0,支付总数)]
        // => [品类，(点击总数,下单总数,支付总数)]

        val clickTuple = clickRes.map((res) => {
            (res._1, (res._2, 0, 0))
        })

        val orderTuple = orderRes.map(res => {
            (res._1, (0, res._2, 0))
        })

        val payTuple = payRes.map(res => {
            (res._1, (0, 0, res._2))
        })

        //将三个数据源合并在一起（取并集能保证全部取到），才能逐个reduceByKey
        val allTuple = clickTuple.union(orderTuple).union(payTuple)
        val Tuple = allTuple.reduceByKey((t1, t2) => {
            (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
        })

        Tuple.sortBy(_._2, false).take(10).foreach(println)
        sc.stop()
    }
}
