package columnrecon;
import com.amazonaws.services.glue.model.JoinType;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ColumnMismatchChecker {

    public static void main(String[] args) {
        // Initialize Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("ColumnMismatchFinderJava")
                .master("local[*]") // Use local mode for development/testing
                .getOrCreate();

        // Sample DataFrames (replace with your actual table loading)
        // For demonstration purposes, let's create some sample data
        List<Row> data1 = Arrays.asList(
                RowFactory.create("A", "X", "P", 100, "NY", "Y"),
                RowFactory.create("B", "Y", "Q", 200, "LA", "Y"),
                RowFactory.create("C", "Z", "R", 300, "CH", "Y"),
                RowFactory.create("D", "W", "S", 400, "SF", "N"), // Inactive
                RowFactory.create("E", "V", "T", 500, "DL", "Y")
        );
        StructType schema1 = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("key_col1", DataTypes.StringType, true),
                DataTypes.createStructField("key_col2", DataTypes.StringType, true),
                DataTypes.createStructField("key_col3", DataTypes.StringType, true),
                DataTypes.createStructField("col_data1", DataTypes.IntegerType, true),
                DataTypes.createStructField("col_data2", DataTypes.StringType, true),
                DataTypes.createStructField("active_in", DataTypes.StringType, true)
        });
        Dataset<Row> df1 = spark.createDataFrame(data1, schema1);

        List<Row> data2 = Arrays.asList(
                RowFactory.create("A", "X", "P", 100, "NY", "Y"),
                RowFactory.create("B", "Y", "Q", 200, "LAX", "Y"), // Mismatch in col_data2
                RowFactory.create("C", "Z", "R", 350, "CH", "Y"), // Mismatch in col_data1
                RowFactory.create("F", "U", "V", 600, "TX", "Y"), // New record in df2
                RowFactory.create("E", "V", "T", 500, "DL", "N")   // Inactive in df2
        );
        StructType schema2 = DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                DataTypes.createStructField("key_col1", DataTypes.StringType, true),
                DataTypes.createStructField("key_col2", DataTypes.StringType, true),
                DataTypes.createStructField("key_col3", DataTypes.StringType, true),
                DataTypes.createStructField("col_data1", DataTypes.IntegerType, true),
                DataTypes.createStructField("col_data2", DataTypes.StringType, true),
                DataTypes.createStructField("active_in", DataTypes.StringType, true)
        });
        Dataset<Row> df2 = spark.createDataFrame(data2, schema2);

        // Define key columns and columns to compare
        List<String> keyColumns = Arrays.asList("key_col1", "key_col2", "key_col3");
        List<String> comparisonColumns = Arrays.asList("col_data1", "col_data2");

        // Filter for active records in both DataFrames
        Dataset<Row> df1Active = df1.filter(col("active_in").equalTo("Y"));
        Dataset<Row> df2Active = df2.filter(col("active_in").equalTo("Y"));

        // Create the join condition with multiple key columns, using aliased columns
        Column joinCondition = functions.lit(true);
        for (String keyCol : keyColumns) {
            joinCondition = joinCondition.and(df1Active.col(keyCol).equalTo(df2Active.col(keyCol)));
        }

        // Perform the full outer join
        Dataset<Row> joinedDf = df1Active.alias("t1").join(df2Active.alias("t2"), joinCondition, JoinType.Outer.toString());

        // Initialize an empty list to store mismatch DataFrames
        List<Dataset<Row>> mismatchDatasets = new ArrayList<>();

        // Iterate through comparison columns to find mismatches
        for (String colName : comparisonColumns) {
            Column mismatchCondition =
                    (col("t1." + colName).isNotNull().or(col("t2." + colName).isNotNull()))
                            .and(col("t1." + colName).notEqual(col("t2." + colName)));

            // Select the mismatched records for the current column
            Dataset<Row> currentColMismatches = joinedDf.filter(mismatchCondition).select(
                    coalesce(col("t1.key_col1"), col("t2.key_col1")).alias("key_col1"),
                    coalesce(col("t1.key_col2"), col("t2.key_col2")).alias("key_col2"),
                    coalesce(col("t1.key_col3"), col("t2.key_col3")).alias("key_col3"),
                    lit(colName).alias("mismatched_col_name"),
                    col("t1." + colName).alias("value_from_t1"),
                    col("t2." + colName).alias("value_from_t2")
            );

            mismatchDatasets.add(currentColMismatches);
        }

        // Union all mismatch DataFrames
        Dataset<Row> mismatchesDf = spark.emptyDataFrame(); // Initialize with an empty DataFrame
        if (!mismatchDatasets.isEmpty()) {
            mismatchesDf = mismatchDatasets.get(0);
            for (int i = 1; i < mismatchDatasets.size(); i++) {
                mismatchesDf = mismatchesDf.union(mismatchDatasets.get(i));
            }
        }

        // Show the results
        System.out.println("Column Level Mismatches:");
        mismatchesDf.show(false);

        // Stop Spark Session
        spark.stop();
    }
}

