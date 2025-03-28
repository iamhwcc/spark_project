package HiveUDF;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author stalwarthuang
 * @since 2025-03-22 星期六 14:37:18
 */
public class testUDF extends UDF {
    public String evaluate(String input) {
        try {
            return "hello" + input;
        } catch (Exception e) {
            e.printStackTrace();
            return "Error";
        }
    }
}
