package RDD

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object RDD_MyPartition {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_MyPartition")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD(List(
            ("Benz", 200),
            ("BMW", 100),
            ("Audi", 300),
            ("BMW", 90),
            ("BMW", 150),
        ))

        // BMW、Benz、Audi分三个分区
        val parRDD = rdd.partitionBy(new MyPartitioner(3))
        val res = parRDD.mapPartitionsWithIndex((index, iter) => {
            iter.map(it => (index,it))
        })
        res.collect().foreach(println)
        sc.stop()
    }

    class MyPartitioner(numPar: Int) extends Partitioner {
        override def numPartitions: Int = numPar

        override def getPartition(key: Any): Int = {
            key match {
                case "Benz" => 0
                case "BMW" => 1
                case "Audi" => 2
            }
        }
    }
}
