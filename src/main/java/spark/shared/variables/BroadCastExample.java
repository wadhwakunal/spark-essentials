package spark.shared.variables;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.reflect.ClassTag;

import java.util.HashMap;
import java.util.Map;

public class BroadCastExample {
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

        HashMap<String,String> genderMap = new HashMap<>();
        genderMap.put("M","Male");
        genderMap.put("F","Female");

        ClassTag<HashMap> tag = scala.reflect.ClassTag$.MODULE$.apply(Map.class);
        Broadcast<HashMap> gender = spark.sparkContext().broadcast(genderMap, tag);

        dataset1.withColumn("Sex",functions.when(dataset1.col("Gender").equalTo("M"),functions.lit(gender.value().get("M"))).otherwise(gender.value().get("F"))).explain();
    }
}
