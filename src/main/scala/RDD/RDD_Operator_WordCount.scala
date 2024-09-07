package RDD

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RDD_Operator_WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Operator_WordCount")
        val sc = new SparkContext(conf)

        //TODO  RDD多种算子多种方法实现WordCount
        println("===========方式1=============")
        WordCount1(sc)
        println("===========方式2==============")
        WordCount2(sc)
        println("===========方式3==============")
        WordCount3(sc)
        println("===========方式4==============")
        WordCount4(sc)
        println("===========方式5==============")
        WordCount5(sc)
        println("===========方式6==============")
        WordCount6(sc)
        println("===========方式7==============")
        WordCount7(sc)
        println("===========方式8==============")
        WordCount8(sc)
        println("===========方式9==============")
        WordCount9(sc)
        sc.stop()
    }

    //核心算子 groupBy
    def WordCount1(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello hwc", "Hello Flink", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val group = words.groupBy(word => word)
        val res = group.mapValues(iter => iter.size)
        res.collect().foreach(println)
    }

    //核心算子 groupByKey (性能差)
    def WordCount2(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello hwc", "Hello Flink", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordKV = words.map(word => (word, 1))
        val group = wordKV.groupByKey()
        val res = group.mapValues(iter => iter.size)
        res.collect().foreach(println)
    }

    //核心算子 reduceByKey
    def WordCount3(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello hwc", "Hello Flink", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordKV = words.map(word => (word, 1))
        val res = wordKV.reduceByKey(_ + _)
        res.collect().foreach(println)
    }

    //核心算子 aggregateByKey
    def WordCount4(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello hwc", "Hello Flink", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordKV = words.map(word => (word, 1))
        val res = wordKV.aggregateByKey(0)(_ + _, _ + _)
        res.collect().foreach(println)
    }

    //核心算子 foldByKey
    def WordCount5(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello hwc", "Hello Flink", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordKV = words.map(word => (word, 1))
        val res = wordKV.foldByKey(0)(_ + _)
        res.collect().foreach(println)
    }

    //核心算子 combineByKey
    def WordCount6(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello hwc", "Hello Flink", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordKV = words.map(word => (word, 1))
        val res = wordKV.combineByKey(v => v, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y)
        res.collect().foreach(println)
    }

    //核心算子 countByKey
    def WordCount7(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello hwc", "Hello Flink", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordKV = words.map(word => (word, 1))
        println(wordKV.countByKey())
    }

    //核心算子 countByValue
    def WordCount8(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello hwc", "Hello Flink", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        println(words.countByValue())
    }
    //核心算子 reduce
    def WordCount9(sc: SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello hwc", "Hello Flink", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val mapWord = words.map(
            word => {
                mutable.Map((word, 1))
            }
        )
        val res = mapWord.reduce(
            (map1, map2) => {
                map2.foreach {
                    //假如此时map1已经有((Hello->2),(Spark->1))
                    //这时候来了个(Flink->1)
                    case (word, count) => {
                        //map1中找不到Flink，所以newCount=0+1=1
                        val newCount = map1.getOrElse(word, 0) + count
                        //map1更新为((Hello->2),(Spark->1),(Flink->1))
                        map1.update(word, newCount)
                    }
                }
                map1
            }
        )
        println(res)
    }
}
