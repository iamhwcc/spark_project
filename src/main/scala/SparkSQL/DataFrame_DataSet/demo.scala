package SparkSQL.DataFrame_DataSet

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object demo {

    case class user(name: String, age: Int)

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("test_dataframe_dataset")
            .config("spark.driver.host","localhost")
            .getOrCreate()

        // RDD converting to Dataframe should import this
        import spark.implicits._

        val df: DataFrame = spark.sparkContext.textFile("/Users/hwc/Documents/Spark Project/Spark_WordCount/datas/people.txt")
            .map(line => {
                val strings: Array[String] = line.split(",")
                user(strings(0), strings(1).trim.toInt)
            }).toDF()

        df.createTempView("user")

        val table: DataFrame = spark.sql(
            """
              |select *
              |from user
              |""".stripMargin)

        val map: Map[String, Any] = table.first().getValuesMap[Any](List("name", "age"))

        println(map)
    }
}
