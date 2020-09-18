package spark.essentials.utilities;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.*;


public class DataPipeline {
    public static void main(String[] args) {
        //Get SparkSession
        SparkSessionUtility sparkSessionUtility = new SparkSessionUtility();
        sparkSessionUtility.setSparkSession();
        SparkSession sparkSession = sparkSessionUtility.getSparkSession();

        final StructType schema = new StructType().add("policyID", DataTypes.IntegerType);

        Dataset<Row> dataset = sparkSession.
                read().
                format("csv").
                option("header", true).
                load("FL_insurance_sample.csv")
                .select("policyID");

        dataset.map((MapFunction<Row,Boolean>) row -> {
            Boolean result = false;
            for(StructField field : schema.fields()){
                DataType T = field.dataType();
                if(T instanceof IntegerType) {
                    if (row.<Integer>getAs(field.name()) != null) {
                        row.<Integer>getAs(field.name());
                        result = true;
                    } else
                        result = false;
                }
            }
            return result;
        },Encoders.BOOLEAN()).show(10,false);

        /*
        Dataset<Row> casted = null;
        for (StructField schemaCol : schema.fields()) {
            casted = dataset.
                    select(schemaCol.name()).
                    withColumn(schemaCol.name() + "_casted", dataset.col(schemaCol.name()).cast(schemaCol.dataType())).
                    withColumn("isValid", functions.lit(true));

            casted = casted.withColumn("isValid",
                    functions.when(casted.col(schemaCol.name()).cast(DataTypes.StringType).equalTo(casted.col(schemaCol.name() + "_casted").cast(DataTypes.StringType)), true)
                            .when(casted.col(schemaCol.name()).cast(DataTypes.StringType).isNull(), false)
                            .otherwise(false)
                            .and(casted.col("isValid")));

            casted = casted.
                    drop(schemaCol.name()).
                    withColumnRenamed(schemaCol.name() + "_casted", schemaCol.name());
        }


        casted.printSchema();
        casted.show(20, false);
        */
    }
}
