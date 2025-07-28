package iceberg;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class IcebergTableWithSnapshots {

    public static void main(String[] args) throws AnalysisException {
        // IMPORTANT: Replace with your actual S3 bucket name
        String s3BucketName = "springbatchtest";
        String tableName = "orders" ;
        String db_table_name = "my_catalog.default.orders" ;
        String tableLocation = String.format("s3a://%s/iceberg_data/%s", s3BucketName,tableName);
        System.out.println("tableLocation:"+tableLocation);

        // Configure SparkSession to use Iceberg with Hadoop Catalog
        SparkSession spark = SparkSession.builder()
                .appName("IcebergTableWithPartitionInS3")
                .master("local[*]") // Use "yarn" or "local[*]" depending on your environment
                .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.my_catalog.type", "hadoop")
                .config("spark.sql.catalog.my_catalog.warehouse", String.format("s3a://%s/iceberg_data", s3BucketName))
                // Configure S3 access using Hadoop's S3A connector
                // Ensure your IAM role/credentials provide access to the S3 bucket
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") // Or your specific provider
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR"); // Suppress verbose Spark logs


        spark.sql("SHOW CATALOGS").show();

        spark.sql("SHOW NAMESPACES IN my_catalog").withColumn("catalog",functions.lit("my_catalog")).show();
        spark.sql("SHOW tables IN my_catalog.default").withColumn("catalog",functions.lit("my_catalog")).show();

        spark.sql("select * from my_catalog.default.orders.partitions").show();
        spark.sql("select * from my_catalog.default.orders.snapshots").show();

        String partitionDate="2025-06-03" ;
        /*
        System.out.println("\n--- Updating a from Table ---");
        Dataset<Row> df = spark.sql("update my_catalog.default.orders set Order_name='I-MAC' where order_id=200 and partition_date='2025-06-03'") ;
        df.show();*/

        Dataset<Row> df =  spark.read()
                .format("iceberg")
                .option("snapshot-id", "3805679732262650128")
                .load("my_catalog.default.orders").where("partition_date='2025-06-03'") ;
        df.show();

        Dataset<Row> df2 =  spark.read()
                .format("iceberg")
                .option("snapshot-id", "3586841250149847650")
                .load("my_catalog.default.orders").where("partition_date='2025-06-03'") ;
        df2.show();


    try {
        // Define schema for the table
        boolean isTblExists= spark.catalog().tableExists(db_table_name);
        if(isTblExists) {
            System.out.println(db_table_name + " Table Already Exists");
            String checkPartition = String.format("select * from my_catalog.default.orders.partitions where partition.partition_date = '%s'", partitionDate);
            Dataset<Row> df1 = spark.sql(checkPartition);
            df1.show();
            if (df1.count() == 0) {
                System.out.println(partitionDate + " partition Not Exists in Table:"+db_table_name+", So Create partition and load ");
                df.writeTo(db_table_name).append();
            } else {
                System.out.println(partitionDate + " partition Exists in Table:"+db_table_name+", So overwriting partition and load. ");
                df.writeTo(db_table_name).overwritePartitions();
            }
        }
        else {
            System.out.println(db_table_name+" Table Not Exists,So Creating Table");
            df.writeTo(db_table_name).partitionedBy(df.col("partition_date")).createOrReplace();
        }
        System.out.println("Data loaded into Iceberg table.:"+db_table_name);

        spark.sql("select * from my_catalog.default.orders.partitions").show();
        spark.sql("select * from my_catalog.default.orders.snapshots").show();


        System.out.println("\n--- Reading Data from Table ---");
        Dataset<Row> readDf1 = spark.table(db_table_name);//.where("partition_date='2025-06-02'") ;
        readDf1.show();

    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        spark.stop();
    }
    }
}
