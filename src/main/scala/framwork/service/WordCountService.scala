package framwork.service

import framwork.common.TService
import framwork.dao.WordCountDao
import org.apache.spark.rdd.RDD

/*
服务层
*/
class WordCountService extends TService{
    private val dao = new WordCountDao()

    //数据分析(逻辑)
    def dataAnalysis() = {
        //从dao层读数据
        val lines = dao.readData("datas/word.txt")
        val words = lines.flatMap(line => line.split(" "))
        val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _).collect()
        wordCount
    }

}
