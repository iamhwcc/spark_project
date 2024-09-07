package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Serializable {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("RDD_Serializable").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "Flink&Storm"))
        val search = new Search("h")
        //search.getMatch1(rdd).collect().foreach(println)
        search.getMatch2(rdd).collect().foreach(println)
        sc.stop()
    }
    //查询对象,用于查询指定的数据
    //query类的构造参数是类的属性，构造参数需要进行闭包检测，等同于类要闭包检测
    class Search(query:String) extends Serializable {
        def isMatch(s: String): Boolean = {
            s.contains(query)
        }
        //如果包含指定数据，就留下
        def getMatch1(rdd: RDD[String]): RDD[String] = {
            rdd.filter(isMatch)
        }
        //如果包含指定数据，就留下
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            rdd.filter(x => x.contains(query))
        }
    }
}
