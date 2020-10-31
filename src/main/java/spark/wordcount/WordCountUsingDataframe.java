package spark.wordcount;

import org.apache.log4j.Level;
import org.apache.spark.sql.*;
import org.apache.log4j.Logger;

public class WordCountUsingDataframe {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();
        Dataset<Row> rawDf = spark
                .read()
                .format("text")
                .load("spark_doc.txt");
        Dataset<Row> splitWordsDf = rawDf
                .select(functions.split(rawDf.col("value")," ").as("words"));
        Dataset<Row> explodedDf = splitWordsDf
                .select(functions.explode(splitWordsDf.col("words")).as("words"));
        Dataset<Row> wordCountDf = explodedDf.groupBy("words").count();
        wordCountDf.show(10,false);
    }
}
