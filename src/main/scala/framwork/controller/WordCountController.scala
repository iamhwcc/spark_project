package framwork.controller

import framwork.common.TController
import framwork.service.WordCountService
import org.apache.spark.rdd.RDD

/*
控制层
*/
class WordCountController extends TController {
    private val service = new WordCountService()

    //调度(处理数据)

    def control(): Unit = {
        val dataResult = service.dataAnalysis()
        dataResult.foreach(println)
    }

}
