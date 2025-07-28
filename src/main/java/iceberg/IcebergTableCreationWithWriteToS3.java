package iceberg;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.CatalogMetadata;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class IcebergTableCreationWithWriteToS3 {

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

        // Dataset<Row> properties = spark.sql("SHOW TBLPROPERTIES my_catalog.default.orders");
        //properties.show();

        //spark.catalog().listCatalogs().show();
        //spark.catalog().listDatabases().show();

        // spark.catalog().setCurrentCatalog("spark_catalog");
        //spark.catalog().setCurrentDatabase("default");
        //spark.catalog().listTables("default").show();

        //  spark.sql("SELECT current_database()").show();


        spark.catalog().listCatalogs().show();
        spark.sql("SHOW CATALOGS").show();

        spark.sql("SHOW NAMESPACES IN my_catalog").withColumn("catalog",functions.lit("my_catalog")).show();
        spark.sql("SHOW NAMESPACES IN spark_catalog").withColumn("catalog",functions.lit("spark_catalog")).show();

        spark.sql("SHOW tables IN my_catalog.default").withColumn("catalog",functions.lit("my_catalog")).show();
        spark.sql("SHOW tables IN spark_catalog.default").withColumn("catalog",functions.lit("spark_catalog")).show();

        spark.sql("select * from my_catalog.default.orders.partitions").show();
        spark.sql("select * from my_catalog.default.orders.snapshots").show();

        // Create some sample data
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("order_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("Order_name", DataTypes.StringType, false),
                DataTypes.createStructField("Order_date", DataTypes.DateType, false)
        });

        List<Row> data = Arrays.asList(
                RowFactory.create(100, "IPHONE", Date.valueOf("2023-01-15")),
                RowFactory.create(200, "IMAC", Date.valueOf("2023-01-16")),
                RowFactory.create(300, "Printers", Date.valueOf("2023-01-17")),
                RowFactory.create(400, "Printers", Date.valueOf("2025-01-17")),
                RowFactory.create(500, "IPAD", Date.valueOf("2025-03-17"))
        );

        Dataset<Row> df = spark.createDataFrame(data, schema);
        String partitionDate= "2025-06-03" ;
        df = df.withColumn("partition_date", functions.lit(partitionDate).cast(DataTypes.DateType)) ;
        df.show();

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


        System.out.println("\n--- Reading Data from Table ---");
        Dataset<Row> readDf = spark.table(db_table_name);//.where("partition_date='2025-06-02'") ;
        readDf.show();

    } catch (Exception e) {
        e.printStackTrace();
    } finally {
        spark.stop();
    }
    }
}
