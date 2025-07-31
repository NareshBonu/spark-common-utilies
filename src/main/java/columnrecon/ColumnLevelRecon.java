package columnrecon;

import com.amazonaws.services.glue.model.JoinType;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.plans.JoinType$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnLevelRecon {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("MismatchFinderCompositeKeyFixed")
                .master("local[*]")
                .getOrCreate();

        // Sample DataFrames
        List<Row> data1 = Arrays.asList(
                RowFactory.create(1, "A", "Alice", 28, "Engineer"),
                RowFactory.create(2, "B", "Bob", 35, "Doctor"),
                RowFactory.create(3, "C", "Charlie", 22, "Student"),
                RowFactory.create(4, "D", "David", 45, "Manager")
        );
        StructType schema1 = new StructType(new StructField[]{
                new StructField("id1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("id2", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("occupation", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> df1 = spark.createDataFrame(data1, schema1);

        List<Row> data2 = Arrays.asList(
                RowFactory.create(1, "A", "Alice", 28, "Engineer"),
                RowFactory.create(2, "B", "Bob", 37, "Doctor"), // Mismatched Age
                RowFactory.create(3, "C", "Charlie", 22, "Student"),
                RowFactory.create(4, "D", "Dave", 45, "Analyst") ,
                RowFactory.create(5, "e", "Naresh", 40, "Analyst")// Mismatched Name & Occupation
        );
        StructType schema2 = new StructType(new StructField[]{
                new StructField("id1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("id2", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("occupation", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> df2 = spark.createDataFrame(data2, schema2);

        List<String> keyColumns = Arrays.asList("id1", "id2"); // Multiple key columns

        // Get common columns, excluding the key columns
        List<String> commonColumns = Arrays.stream(df1.columns())
                .filter(col -> !keyColumns.contains(col) && Arrays.asList(df2.columns()).contains(col))
                .collect(Collectors.toList());

        // Alias the DataFrames before joining
        Dataset<Row> df1Aliased = df1.alias("df1");
        Dataset<Row> df2Aliased = df2.alias("df2");

        // Create the join condition with multiple key columns, using aliased columns
        Column joinCondition = functions.lit(true);
        for (String keyCol : keyColumns) {
            joinCondition = joinCondition.and(df1Aliased.col(keyCol).equalTo(df2Aliased.col(keyCol)));
        }

        // Perform the full outer join
        Dataset<Row> joinedDf = df1Aliased.join(df2Aliased, joinCondition, JoinType.Outer.toString());

        // Dynamically compare columns and create a new column for mismatches
        // When selecting key columns, use one of the aliased DataFrames (e.g., df1Aliased)
        // or specifically reference them to avoid the ambiguity
        List<Column> selectColumns = keyColumns.stream()
                .map(df1Aliased::col) // Use df1Aliased for key columns
                .collect(Collectors.toList());

        selectColumns.addAll(
                commonColumns.stream()
                        .map(colName -> functions.when(
                                functions.not(df1Aliased.col(colName).equalTo(df2Aliased.col(colName))), // Qualify references
                                functions.concat_ws(" vs ", df1Aliased.col(colName), df2Aliased.col(colName)) // Qualify references
                        ).alias(colName + "_mismatch"))
                        .collect(Collectors.toList())
        );

        Dataset<Row> mismatchedDf = joinedDf.select(
                selectColumns.toArray(new Column[0])
        );

        // Filter for rows with mismatches (at least one mismatch column is not null)
        mismatchedDf = mismatchedDf.filter(
                commonColumns.stream()
                        .map(colName -> functions.col(colName + "_mismatch").isNotNull())
                        .reduce(functions.lit(false), Column::or)
        );

        System.out.println("Mismatched rows:");
        mismatchedDf.show();

        spark.stop();
    }
}
