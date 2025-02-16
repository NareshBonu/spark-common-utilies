package SCD2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;

import java.util.Arrays;
import java.util.List;

public class SCD2Implementation_Multiple_KeyColumns {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        Logger.getLogger("org.*").setLevel(Level.OFF);

        SparkSession spark = SparkSession.builder()
                .appName("SCD2 Implementation")
                .master("local[*]")
                .getOrCreate();

        // Sample existing SCD2 table (dimension table)
        List<Row> existingData = Arrays.asList(
                RowFactory.create(1, "A", "Category1", "2024-01-01", "9999-12-31", "Active"),
                RowFactory.create(2, "B", "Category2", "2024-01-01", "9999-12-31", "Active")
        );
        StructType schema = new StructType()
                .add("id", "int")
                .add("name", "string")
                .add("category", "string")
                .add("start_date", "string")
                .add("end_date", "string")
                .add("status", "string");
        Dataset<Row> masterTable = spark.createDataFrame(existingData, schema);

        // Incoming DataFrame (new data from source)
        List<Row> incomingData = Arrays.asList(
                RowFactory.create(1, "A", "Category1"),  // Unchanged
                RowFactory.create(2, "B", "CategoryX"),  // Changed
                RowFactory.create(3, "C", "Category3"),   // New record
                RowFactory.create(4, "D", "Category4")   // New record
        );
        StructType incomingSchema = new StructType()
                .add("id", "int")
                .add("name", "string")
                .add("category", "string");
        Dataset<Row> deltaTable = spark.createDataFrame(incomingData, incomingSchema);

        // Define key columns and ignore columns
        List<String> keyColumns = Arrays.asList("id", "name");
        List<String> ignoreColumns = Arrays.asList("start_date", "end_date", "status");

        // Join existing and incoming data on key columns
        Column joinCondition = keyColumns.stream()
                .map(colName -> masterTable.col(colName).equalTo(deltaTable.col(colName)))
                .reduce(Column::and).orElse(functions.lit(true));

        Dataset<Row> joined = deltaTable.join(masterTable, joinCondition, "left_outer");
        System.out.println("joined");
        joined.show();

        // Identify new and changed records
        Column changeCondition = Arrays.stream(deltaTable.schema().fieldNames())
                .filter(col -> !keyColumns.contains(col) && !ignoreColumns.contains(col))
                .map(col -> deltaTable.col(col).notEqual(masterTable.col(col)))
                .reduce(Column::or).orElse(functions.lit(false));

        Column changeCondition1 = Arrays.stream(deltaTable.schema().fieldNames())
                .filter(col -> !keyColumns.contains(col) && !ignoreColumns.contains(col))
                .map(col -> deltaTable.col(col).equalTo(masterTable.col(col)))
                .reduce(Column::or).orElse(functions.lit(false));

        Dataset<Row> changedRecords = joined.filter(changeCondition);
        System.out.println("changedRecords");
        changedRecords.show();

        Dataset<Row> updatedActive = changedRecords.select(deltaTable.col("*")) ;
        System.out.println("updatedActive");
        updatedActive.show();

        System.out.println("expiredRecords");
        Dataset<Row> expiredRecords  = changedRecords.select(masterTable.col("*")) ;
        expiredRecords= expiredRecords.withColumn("end_date",functions.current_date())
                        .withColumn("status",functions.lit("InActive")) ;
        expiredRecords.show();

        Column changeCondition3 = Arrays.stream(masterTable.schema().fieldNames())
                .filter(col -> keyColumns.contains(col) )
                .map(col -> masterTable.col(col).isNull())
                .reduce(Column::or).orElse(functions.lit(false));

        Dataset<Row> newRecords = joined.filter(changeCondition3) ;     //(masterTable.col("ID").isNull().and(masterTable.col("name").isNull()));
        newRecords = newRecords.select(deltaTable.col("id"), deltaTable.col("name"), deltaTable.col("category"));
        //Dataset<Row> newRecords = deltaTable.except(masterTable.selectExpr("id", "name", "category"));
        System.out.println("newRecords");
        newRecords.show();

        // Set validity dates and status

        Dataset<Row> activeNewRecords = updatedActive.union(newRecords) // New + Updated wth Columns
                .withColumn("start_date", functions.current_date())
                .withColumn("end_date", functions.lit("9999-12-31"))
                .withColumn("status", functions.lit("Active"));

        //unchanged records
        Dataset<Row> unchangedRecords = joined.filter(changeCondition1);
        unchangedRecords = unchangedRecords.select(masterTable.col("id"), masterTable.col("name"), masterTable.col("category"), masterTable.col("start_date"), masterTable.col("end_date"), masterTable.col("status"));
        System.out.println("unchangedRecords");
        unchangedRecords.show();


        // Final Dimension Table (Union of old, updated, and new records)
        Dataset<Row> finalDimTable = unchangedRecords.union(activeNewRecords).union(expiredRecords);


        // Show results
        finalDimTable.show();
        spark.stop();
    }
}

