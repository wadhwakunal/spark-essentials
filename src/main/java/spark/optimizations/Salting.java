package spark.optimizations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Salting {
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

        dataset1 = dataset1.withColumn("join_id", functions.concat(dataset1.col("Emp_ID"), functions.floor(functions.rand().multiply(10))));
        dataset1.show(10, false);

        Dataset<Row> dataset2 = spark
                .read()
                .format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .load("JoinDataset2.csv");

        int[] randVal = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        dataset2 = dataset2.withColumn("join_id", functions.explode(functions.lit(randVal)));
        dataset2 = dataset2.withColumn("join_id", functions.concat(dataset2.col("Emp_ID"), dataset2.col("join_id")));
        dataset1.join(dataset2, "join_id").show(10, false);
    }
}
