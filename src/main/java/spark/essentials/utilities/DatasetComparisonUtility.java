package spark.essentials.utilities;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;


public class DatasetComparisonUtility {
    public static void main(String[] args) {
        //Compare two datasets and identify the differences between two datasets

        Logger.getLogger("org").setLevel(Level.ERROR);
        DatasetComparisonUtility datasetComparisonUtility = new DatasetComparisonUtility();

        SparkSession spark = SparkSession
                .builder()
                .appName("DatasetComparisonUtility")
                .master("local")
                .getOrCreate();

        Dataset<Row> firstDataset = spark
                .read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("FL_insurance_sample.csv");

        Dataset<Row> secondDataset = spark
                .read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("FL_insurance_sample1.csv");

        //1. Compare the field-names and field-datatypes
        datasetComparisonUtility.schemaComparison(firstDataset, secondDataset);

        //2. Compare the number of rows
        datasetComparisonUtility.rowComparison(firstDataset, secondDataset);

        //3. Compare all the fields
        String uniqueIdentifier[] = {"policyID"};
        datasetComparisonUtility.fieldComparison(uniqueIdentifier,firstDataset,secondDataset);
    }

    public void schemaComparison(Dataset<Row> dataset1, Dataset<Row> dataset2) {
        //This function checks:
        //Number of columns in both the datasets
        //Name of columns in both the datasets as per sequence
        //DataType of columns in both the datasets as per sequence
        String[] schema_Dataset1 = dataset1.columns();
        String[] schema_Dataset2 = dataset2.columns();

        if (schema_Dataset1.length != schema_Dataset2.length) {
            System.out.println("Number of columns in the datasets are unequal; hence exiting");
            System.exit(1);
        }

        for (int i = 0; i < schema_Dataset1.length; i++) {
            if (!schema_Dataset1[i].equals(schema_Dataset2[i])) {
                System.out.println("Column Name in position " + i + " mismatched in the datasets[" + schema_Dataset1[i] + "," + schema_Dataset2[i] + "]; hence exiting");
                System.exit(1);
            }
            if (!dataset1.schema().fields()[i].dataType().typeName().equals(dataset2.schema().fields()[i].dataType().typeName())) {
                System.out.println("Column Type in position " + i + " mismatched in the datasets[" + dataset1.schema().fields()[i].dataType().typeName() + "," + dataset2.schema().fields()[i].dataType().typeName() + "]; hence exiting");
                System.exit(1);
            }
        }
    }

    public void rowComparison(Dataset<Row> dataset1, Dataset<Row> dataset2) {
        //This function checks:
        //Number of rows in both the datasets
        if (dataset1.count() != dataset2.count()) {
            System.out.println("Number of rows in the datasets are unequal; hence exiting");
            System.exit(1);
        }
    }

    public void fieldComparison(String[] uniqueIdentifier, Dataset<Row> dataset1, Dataset<Row> dataset2) {
        //This function checks:
        //Field values in both the datasets
        String[] requiredColumns = new String[uniqueIdentifier.length+1];
        for(int i=0;i<uniqueIdentifier.length;i++)
            requiredColumns[i] = uniqueIdentifier[i];
        ArrayList<Dataset<Row>> array_differenceDataset = new ArrayList<>();
        ArrayList<Dataset<Row>> array_Dataset = new ArrayList<>();
        for (String field : dataset1.columns()) {
            requiredColumns[uniqueIdentifier.length] = field;
            array_differenceDataset.add(dataset1.selectExpr(requiredColumns).except(dataset2.selectExpr(requiredColumns)));
            array_Dataset.add(dataset2.selectExpr(requiredColumns).withColumn("comparedColumn",org.apache.spark.sql.functions.lit(field)));
        }
        for (int i = 0; i < array_differenceDataset.size(); i++) {
            if (array_differenceDataset.get(i).count() > 0) {
                System.out.println(array_differenceDataset.get(i).join(array_Dataset.get(i),JavaConverters.asScalaIteratorConverter(Arrays.asList(uniqueIdentifier).iterator()).asScala().toSeq()).collectAsList().get(0));
            }
        }
    }
}
