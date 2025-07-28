package iceberg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.RowFactory;

import java.util.Arrays;
import java.util.List;

public class IcebergS3HadoopCatalogExample {

    public static void main(String[] args) {
        // IMPORTANT: Replace with your actual S3 bucket name
        String s3BucketName = "springbatchtest";

        String tableLocation = String.format("s3a://%s/iceberg_data/my_sample_table", s3BucketName);

        // Configure SparkSession to use Iceberg with Hadoop Catalog
        SparkSession spark = SparkSession.builder()
                .appName("IcebergS3HadoopCatalogExample")
                .master("local[*]") // Use "yarn" or "local[*]" depending on your environment
              //  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                // Configure the Hadoop Catalog. This tells Iceberg where to find table metadata.
                .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
                .config("spark.sql.catalog.hadoop_catalog.warehouse", String.format("s3a://%s/iceberg_data", s3BucketName))
                // Configure S3 access using Hadoop's S3A connector
                // Ensure your IAM role/credentials provide access to the S3 bucket
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") // Or your specific provider
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR"); // Suppress verbose Spark logs

        try {

            Dataset<Row> df12 = spark.sql("SELECT * FROM hadoop_catalog.my_sample_table.snapshots");
            df12.show(false) ;
            spark.sql("SELECT * FROM hadoop_catalog.my_sample_table.history").show();

            spark.sql("DESCRIBE TABLE hadoop_catalog.my_sample_table").show();

            System.exit(0);

             spark.sql("update hadoop_catalog.my_sample_table set name='naresh' where id=5") ;
            System.out.println("--- Updated Iceberg Table:my_sample_table ---");
            Dataset<Row> df1 = spark.sql("select * from hadoop_catalog.my_sample_table");
            df1.show();


            System.out.println("--- Creating Iceberg Table ---");
            // Define schema for the table
            spark.sql(String.format(
                    "CREATE TABLE hadoop_catalog.my_sample_table (id INT, name STRING, created_at TIMESTAMP) " +
                            "USING iceberg " +
                            "PARTITIONED BY (days(created_at)) " +
                            "LOCATION '%s'", tableLocation));
            System.out.println("Iceberg table ' ' created successfully at: " + tableLocation);

            System.out.println("\n--- Loading Data into Table ---");
            // Create some sample data
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, false),
                    DataTypes.createStructField("created_at", DataTypes.TimestampType, false)
            });

            List<Row> data = Arrays.asList(
                    RowFactory.create(1, "Alice", java.sql.Timestamp.valueOf("2023-01-15 10:00:00")),
                    RowFactory.create(2, "Bob", java.sql.Timestamp.valueOf("2023-01-16 11:30:00")),
                    RowFactory.create(3, "Charlie", java.sql.Timestamp.valueOf("2023-01-17 14:45:00")),
                    RowFactory.create(4, "David", java.sql.Timestamp.valueOf("2023-01-17 15:00:00"))
            );

            Dataset<Row> df = spark.createDataFrame(data, schema);

            // Write data to the Iceberg table
            df.writeTo("hadoop_catalog.my_sample_table").append();
            System.out.println("Data loaded into Iceberg table.");

            System.out.println("\n--- Reading Data from Table ---");

            Dataset<Row> readDf = spark.table("hadoop_catalog.my_sample_table") ;
            readDf.show();


            System.out.println("\n--- Performing a Time Travel Query (Example) ---");
            // Load more data to create another snapshot
            List<Row> moreData = Arrays.asList(
                    RowFactory.create(5, "Eve", java.sql.Timestamp.valueOf("2023-01-18 09:00:00"))
            );
            Dataset<Row> moreDf = spark.createDataFrame(moreData, schema);
            moreDf.writeTo("hadoop_catalog.my_sample_table").append();
            System.out.println("More data loaded to create a new snapshot.");

            // Read data as of a previous snapshot (time travel)
            // Note: For actual time travel, you'd usually get the timestamp of a previous commit.
            // For this example, we'll just demonstrate the syntax.
            // You can find snapshot IDs or timestamps by inspecting the Iceberg metadata directly in S3.
            System.out.println("\nReading data from a previous state (example using a timestamp):");
            try {
                // Adjust this timestamp based on your actual data loading times
                // This will read the table state as of a timestamp before the 'Eve' record was added.
                spark.table("hadoop_catalog.my_sample_table").show();
                        //.option("as-of-timestamp", "1673977500000").load().show();
                // 1673977500000L is roughly Jan 17, 2023 16:35:00 GMT+05:30
            } catch (Exception e) {
                System.err.println("Could not perform time travel query. Make sure the timestamp is valid and a prior snapshot exists: " + e.getMessage());
            }

            System.out.println("\n--- Dropping Table (Optional) ---");
            // Uncomment the line below to drop the table and its metadata/data from S3
            // spark.sql("DROP TABLE hadoop_catalog.my_sample_table");
            // System.out.println("Iceberg table 'my_sample_table' dropped successfully.");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
