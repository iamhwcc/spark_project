package SparkSQL.DataFrame_DataSet

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object from_mysql {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("test_dataframe_dataset")
            .config("spark.driver.host", "localhost")
            .getOrCreate()
        val properties = new Properties()
        properties.put("user", "root")
        properties.put("password", "hwc26499773")
        properties.put("driver", "com.mysql.cj.jdbc.Driver")

        val df: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/leetcode", "tb_order_detail", properties)
        df.createTempView("tb_order_detail")
        val sql: String =
            """
              |select product_id, sum(price)
              |from tb_order_detail
              |group by product_id
              |""".stripMargin
        spark.sql(sql).show()
    }
}
