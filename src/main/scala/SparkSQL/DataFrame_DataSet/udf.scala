package SparkSQL.DataFrame_DataSet

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object udf {

    case class user(name: String, salary: Long)

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("test_dataframe_dataset")
            .config("spark.driver.host", "localhost")
            .getOrCreate()

        import spark.implicits._

        val df: Dataset[user] = spark.read.json("/Users/hwc/Documents/Spark Project/Spark_WordCount/datas/employees.json").as[user]

        df.createTempView("user")

        spark.udf.register("myConcat", (name: String) => {
            "EmployeeName: " + name
        })


        spark.udf.register("avgSalary", (salary: Long) => {
            salary / 2
        })

        val sql: String = """
                               |select name, myConcat(name), salary, avgSalary(salary)
                               |from user
                               |""".stripMargin

        spark.sql(sql).show(false)


        spark.close()
    }

}
