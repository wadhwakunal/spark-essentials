package spark.essentials.utilities;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class HandleMultipleDelimiters {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        JavaRDD<Row> rawRDD = spark
                .read()
                .format("text")
                .load("JoinDataset2.txt")
                .toJavaRDD()
                .map(line -> {
                            String[] fields = line.mkString(",").replace("|", ",").split(",");
                            return RowFactory.create(Integer.parseInt(fields[0]), fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]);
                        }
                );

        StructType schema = new StructType(new StructField[]{
                new StructField("Emp_ID", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("Name_Prefix", DataTypes.StringType, false, Metadata.empty()),
                new StructField("First_Name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Middle_Initial", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Last_Name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Gender", DataTypes.StringType, false, Metadata.empty()),
                new StructField("E_Mail", DataTypes.StringType, false, Metadata.empty())
        });

        spark.createDataFrame(rawRDD, schema).show(false);
    }
}
