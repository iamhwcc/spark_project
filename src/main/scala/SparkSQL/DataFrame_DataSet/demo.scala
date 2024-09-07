package SparkSQL.DataFrame_DataSet

import org.apache.spark.sql.{DataFrame, SparkSession}

object demo {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("test_dataframe_dataset")
            .config("spark.driver.host","localhost")
            .getOrCreate()
        val df: DataFrame = spark.read.json("/Users/hwc/Documents/Spark Project/Spark_WordCount/datas/employees.json")
        df.createTempView("user")
        val sql: String = """
                               |select *
                               |from user
                               |""".stripMargin
        spark.sql(sql).show()
    }
}
