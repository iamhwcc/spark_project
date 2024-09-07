package RDD

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Action {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("RDD_Action").setMaster("local[*]")
        val sc = new SparkContext(conf)

        //        val rdd = sc.makeRDD(List(1, 2, 3, 4))
        //        println(rdd.reduce(_ + _))
        //        println(rdd.count())

        //aggregate
        //        val newRDD = rdd.aggregate(0)(_ + _, _ + _)
        //        println(newRDD)

        //countByKey & countByValue
        //        val rdd = sc.makeRDD(List(1, 2, 3, 4, 4, 3, 2, 2, 2, 2, 1, 1, 3))
        //        val cnt = rdd.countByValue()
        //        println(cnt)
        //
        //        val rdd1 = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1), ("b", 2), ("c", 3), ("c", 3)))
        //        println(rdd1.countByKey())

        val rdd = sc.makeRDD(List(1, 2, 3, 4))
        val user = new User()
        rdd.foreach(
            num => {
                println("age = " + (user.age + num))
            }
        )
        sc.stop()
    }
    class User extends Serializable {
        var age = 30
    }
}
