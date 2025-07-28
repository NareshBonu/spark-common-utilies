/*
package iceberg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class IcebergTableWithS3AndGlueDB {

    public static void main(String[] args) {
        // IMPORTANT: Replace with your actual S3 bucket name
        String s3BucketName = "springbatchtest";


        // Configure SparkSession to use Iceberg with Hadoop Catalog
        SparkSession spark = SparkSession.builder()
                .appName("IcebergTableWithS3AndGlueDB")
                .master("local[*]") // Use "yarn" or "local[*]" depending on your environment
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.glue_catalog.warehouse", String.format("s3a://%s/iceberg_data", s3BucketName))
                .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .config("spark.sql.catalog.glue_catalog.io-impl=", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") // Or your specific provider
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR"); // Suppress verbose Spark logs

        try {

            String tableName="glue_catalog.naresh_db.customer" ;
            spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.naresh_db");

            spark.sql("""
                        CREATE TABLE glue_catalog.naresh_db.customer 
                        (
                        cust_id INT,
                        cust_name STRING,
                        record_eff_date DATE
                        )
                        USING iceberg
                        PARTITIONED BY (record_eff_date)
                        TBLPROPERTIES ('format-version' = '2')
                """
            );

            System.out.println("Iceberg table ' ' created successfully at: " + tableName);

            spark.sql("DESCRIBE TABLE glue_catalog.naresh_db.customer ").show();

            System.out.println("\n--- Loading Data into Table ---");
            // Create some sample data
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("cust_id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("cust_name", DataTypes.StringType, false),
                    DataTypes.createStructField("record_eff_date", DataTypes.DateType, false)
            });

            List<Row> data = Arrays.asList(
                    RowFactory.create(1, "Alice", java.sql.Date.valueOf("2023-01-15")),
                    RowFactory.create(2, "Bob", java.sql.Timestamp.valueOf("2023-01-16")),
                    RowFactory.create(3, "Charlie", java.sql.Timestamp.valueOf("2023-01-17")),
                    RowFactory.create(4, "David", java.sql.Timestamp.valueOf("2023-01-17"))
            );

            Dataset<Row> df = spark.createDataFrame(data, schema);

            // Write data to the Iceberg table
            df.writeTo(tableName).append();
            System.out.println("Data loaded into Iceberg table.");

            System.out.println("\n--- Reading Data from Table ---");
            Dataset<Row> readDf = spark.table(tableName) ;
            readDf.show();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
*/
