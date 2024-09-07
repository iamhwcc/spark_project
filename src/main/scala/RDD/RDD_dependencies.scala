package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_dependencies {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_dependencies")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("datas/word.txt")
        //println(lines.toDebugString)//打印血缘关系
        println(lines.dependencies)//打印依赖关系
        println("*********************")
        val words: RDD[String] = lines.flatMap(line => line.split(" "))
        //println(words.toDebugString) //打印血缘关系
        println(words.dependencies)//打印依赖关系
        println("*********************")
        val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)
        //println(wordCount.toDebugString) //打印血缘关系
        println(wordCount.dependencies)//打印依赖关系
        println("*********************")
        wordCount.collect().foreach(println)
        sc.stop()
    }

}
