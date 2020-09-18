package spark.essentials.utilities;

import org.apache.spark.sql.*;
public class SparkSessionUtility {
    private SparkSession sparkSession;

    public void setSparkSession(){
        sparkSession = SparkSession.
                builder().
                master("local").
                appName("validateCSV").
                getOrCreate();
    }

    public SparkSession getSparkSession(){
        return sparkSession;
    }
}
