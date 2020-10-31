package spark.wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountExampleUsingRDD {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Word Count using RDD");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("spark_doc.txt");

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordCounter = words.mapToPair(element -> new Tuple2<>(element, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        wordCounter.collect().forEach(System.out::println);
    }
}
