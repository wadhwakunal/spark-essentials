package spark.essentials.utilities;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SizeEstimator;

public class SizeEstimatorExample {
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

        System.out.println(SizeEstimator.estimate(spark));
        System.out.println(SizeEstimator.estimate(dataset1));
    }
}
