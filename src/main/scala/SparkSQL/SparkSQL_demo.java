package SparkSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;


/**
 * @author stalwarthuang
 * @since 2024-07-30 星期二 10:31:05
 */
public class SparkSQL_demo {
    public static void main(String[] args) {
        // 构建Spark SQL环境 (构建器模式)
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("spark_sql").getOrCreate();
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "hwc26499773");
        Dataset<Row> data = sparkSession.read().jdbc("jdbc:mysql://localhost:3306", "leetcode.tb_video_info", properties);
        data.select("tag").show();

        sparkSession.close();
    }
}
