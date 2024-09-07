package RDD

import org.apache.spark.{SparkConf, SparkContext}

object checkPointAndPersist {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Persist")
        val sc = new SparkContext(conf)

        val list = List("Hello Scala","Hello Spark","Hello Flink")
        val rdd = sc.makeRDD(list)
        val rdd1 = rdd.flatMap(_.split(" "))
        val rdd2 = rdd1.map((_,1))

        //cache：将数据临时存储在内存中进行重用（不安全）
        //       不会切断血缘关系
        //persist：将数据临时存储在磁盘文件中进行重用（安全，但性能低）
        //cache和persist如果作业执行完毕，临时文件就丢失
        //checkpoint：将数据长久地保存在磁盘中进行重用（安全，但性能低）
        //            为了保证数据安全，checkpoint一般会再独立执行一遍作业
        //综合考虑，cache和checkpoint联合使用是好的
        //checkpoint会切断血缘关系，重新建立新的血缘关系，等同于改变了数据源

        val res1 = rdd2.reduceByKey(_ + _)
        res1.collect().foreach(println)
        println("************************")
        val res2 = rdd2.groupByKey()
        res2.collect().foreach(println)
        sc.stop()
    }

}
