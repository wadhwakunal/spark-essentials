package spark.essentials.utilities;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public class DataframeSizeExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        Dataset<Row> dataset1 = spark
                .read()
                .format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load("JoinDataset1.csv");

        //dataset1.cache();
        LogicalPlan logical = dataset1.queryExecution().logical();
        System.out.println(spark.sessionState().executePlan(logical).optimizedPlan().stats().sizeInBytes());
    }
}
