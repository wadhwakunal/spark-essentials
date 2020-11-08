package spark.essentials.utilities;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class AssignSchemaExample {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("AssignSchemaExample")
                .getOrCreate();

        StructType dataset_Schema = new StructType(new StructField[]{
                new StructField("emp_id", DataTypes.LongType,false, Metadata.empty()),
                new StructField("name_prefix",DataTypes.StringType,false,Metadata.empty()),
                new StructField("first_name",DataTypes.StringType,false,Metadata.empty()),
                new StructField("middle_initial",DataTypes.StringType,false,Metadata.empty()),
                new StructField("last_name",DataTypes.StringType,false,Metadata.empty()),
                new StructField("gender",DataTypes.StringType,false,Metadata.empty()),
                new StructField("email",DataTypes.StringType,false,Metadata.empty())
        });

        StructType dataset_Schema2 = new StructType()
                .add("emp_id", DataTypes.IntegerType,false, Metadata.empty())
                .add("name_prefix",DataTypes.StringType,false,Metadata.empty())
                .add("first_name",DataTypes.StringType,false,Metadata.empty())
                .add("middle_initial",DataTypes.StringType,false,Metadata.empty())
                .add("last_name",DataTypes.StringType,false,Metadata.empty())
                .add("gender",DataTypes.StringType,false,Metadata.empty())
                .add("email",DataTypes.StringType,false,Metadata.empty());

        Dataset<Row> dataset = sparkSession
                .read()
                .format("csv")
                .option("header",true)
                .schema(dataset_Schema)
                .load("JoinDataset1.csv");

        dataset.show(10,false);
        dataset.printSchema();

        Dataset<Row> dataset2 = sparkSession
                .read()
                .format("csv")
                .option("header",true)
                .schema(dataset_Schema2)
                .load("JoinDataset1.csv");

        dataset2.show(10,false);
        dataset2.printSchema();
    }
}
