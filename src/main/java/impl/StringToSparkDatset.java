package impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class StringToSparkDatset {
    public static void main(String[] args) {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("String to Dataset Example")
                .master("local[*]") // For local testing
                .getOrCreate();

        // String to be saved
        String multiLineString = "Name\nHello, Spark!\nThis is a multi-line string.\nEach line becomes a row.";

        List<String> list = Arrays.asList(multiLineString.split("\n"));
        System.out.println(list);
        // Convert string to Dataset<String>
        Dataset<String> dataset = spark.createDataset(
                list, // Convert string to List
                org.apache.spark.sql.Encoders.STRING()
        );

        // Show dataset
        dataset.show();

        Dataset<Row> dataset1 = spark.read().option("header","true").csv(dataset);
        dataset1.show(false);



        // Stop Spark session
        spark.stop();
    }
}