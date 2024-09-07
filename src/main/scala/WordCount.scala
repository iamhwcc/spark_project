import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        //配置SparkConf和SparkContext上下文
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        //val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
        val sc = new SparkContext(conf)
        //文件路径，若file:///则是linux虚拟机内的文件
        //最好用hdfs上的文件hdfs://192.168.142.100:9000
        val filePath = "hdfs://192.168.142.100:9000/a.txt"
        //获取一行一行内容，RDD
        val lines: RDD[String] = sc.textFile(filePath)
        //扁平化处理，将整体拆分成个体，获得一个一个单词，RDD
        val words: RDD[String] = lines.flatMap(line => line.split(" "))
        //map获得(word,1)的形式，reduceByKey对相同的key对应的value进行累加,RDD
        val wordCount: RDD[(String, Int)] = words.map(word => (word, 1)).reduceByKey(_ + _)
        //输出结果
        wordCount.collect().foreach(println)
    }
}
