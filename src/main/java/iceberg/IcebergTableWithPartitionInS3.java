package iceberg;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class IcebergTableWithPartitionInS3 {

    public static void main(String[] args) {
        // IMPORTANT: Replace with your actual S3 bucket name
        String s3BucketName = "springbatchtest";
        String tableName = "customer_table" ;
        String db_table_name = "my_ib_catalog.customer_table" ;
        String tableLocation = String.format("s3a://%s/iceberg_data/%s", s3BucketName,tableName);
        System.out.println("tableLocation:"+tableLocation);

        // Configure SparkSession to use Iceberg with Hadoop Catalog
        SparkSession spark = SparkSession.builder()
                .appName("IcebergTableWithPartitionInS3")
                .master("local[*]") // Use "yarn" or "local[*]" depending on your environment
              //  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                // Configure the Hadoop Catalog. This tells Iceberg where to find table metadata.
                .config("spark.sql.catalog.my_ib_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.my_ib_catalog.type", "hadoop")
                .config("spark.sql.catalog.my_ib_catalog.warehouse", String.format("s3a://%s/iceberg_data", s3BucketName))
                // Configure S3 access using Hadoop's S3A connector
                // Ensure your IAM role/credentials provide access to the S3 bucket
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") // Or your specific provider
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR"); // Suppress verbose Spark logs

        try {
            // Define schema for the table
            boolean isTblExists= spark.catalog().tableExists(db_table_name);
            if(isTblExists)
                System.out.println(db_table_name+" Table Already Exists");
            else {
                System.out.println(db_table_name+" Table Not Exists,So Creating Table");
                spark.sql(String.format(
                        "CREATE TABLE IF NOT EXISTS %s (id INT, name STRING, dob DATE,partition_date DATE) " +
                                "USING iceberg " +
                                "PARTITIONED BY (partition_date) " +
                                "LOCATION '%s'", db_table_name, tableLocation));
                System.out.println("Iceberg table 'customer_table' created successfully at: " + db_table_name);
            }

            System.out.println("\n--- Loading Data into Table ---");
            // Create some sample data
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false),
                    DataTypes.createStructField("dob", DataTypes.DateType, false)
            });

            List<Row> data = Arrays.asList(
                    RowFactory.create(4, "Naresh", java.sql.Date.valueOf("2023-01-15")),
                    RowFactory.create(5, "Saranya", java.sql.Date.valueOf("2023-01-16")),
                    RowFactory.create(6, "Dashan", java.sql.Date.valueOf("2023-01-17")),
                    RowFactory.create(7, "Bonu", java.sql.Date.valueOf("2023-01-17"))
            );

            Dataset<Row> df = spark.createDataFrame(data, schema);
            df = df.withColumn("partition_date", functions.lit("2025-06-02").cast(DataTypes.DateType)) ;

            System.out.println("--- Checking partition Exists or not using Spark SQL on .partitions ---");
            String partition_value= "2025-06-01";
            //Date targetDate = Date.valueOf(partition_value);
            String sqlQuery = String.format(
                    "SELECT count(*) FROM %s.partitions where  partition.partition_date =  CAST ('%s' as DATE) ",
                    db_table_name, partition_value ) ;

            long countSQL = spark.sql(sqlQuery).head().getLong(0);

            if (countSQL > 0) {
                System.out.println("Partition for date '" + partition_value + "' EXISTS (using SQL).SO Overwriting ");
                // Write data to the Iceberg table
                df.writeTo(db_table_name).overwritePartitions();
            } else {
                System.out.println("Partition for date '" + partition_value + "' DOES NOT EXIST (using SQL).So Appending");
                // Write data to the Iceberg table
                df.writeTo(db_table_name).append();
            }
            System.out.println("Data loaded into Iceberg table.:"+db_table_name);

            System.out.println("\n--- Reading Data from Table ---");
            Dataset<Row> readDf = spark.table(db_table_name).where("partition_date='2025-06-01'") ;
            readDf.show();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
