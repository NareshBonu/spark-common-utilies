package SCD2;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import util.SparkCommons;

import java.util.Arrays;
import java.util.List;

public class SCD2Implemention {
    private static SparkSession sparkSession;
    public static void main(String[] args){


        sparkSession = SparkCommons.createSparkSession("", "");
        Dataset<Row> masterDF= prepareMasterDF();
        Dataset<Row> deltaDF= prepareDeltaDF();

        Column join_Condition = masterDF.col("ID").equalTo(deltaDF.col("ID")) ;
        Dataset<Row> joinedDF = deltaDF.join(masterDF, join_Condition, "left_outer");
        joinedDF.show();
        //Filter for new or changed records
        Dataset<Row> changed_or_newDF = joinedDF.filter(masterDF.col("ID").isNull().or(
                masterDF.col("CITY").notEqual(deltaDF.col("CITY"))));


        changed_or_newDF =  changed_or_newDF.select(deltaDF.col("ID"),deltaDF.col("NAME"),deltaDF.col("CITY")) ;
        Dataset<Row> new_recordsDF =  changed_or_newDF
                .withColumn("EFF_DT", functions.date_format(functions.current_date(),"yyyy-MM-dd"))
                .withColumn("END_DT", functions.lit("9999-12-31"))
                .withColumn("ACT_IN", functions.lit("Y")) ;
        new_recordsDF.show();
        //Mark old records as inactive
        Dataset<Row>    inactive_recordsDF = masterDF.join(changed_or_newDF.select("id").distinct(), "ID", "inner");
        inactive_recordsDF.show();
        inactive_recordsDF =inactive_recordsDF
                .withColumn("END_DT", functions.date_format(functions.current_date(),"yyyy-MM-dd"))
                .withColumn("ACT_IN", functions.lit("N"))
                ;
        inactive_recordsDF.show();
        Dataset<Row>  finalDF = masterDF.union(new_recordsDF).union(inactive_recordsDF);
        finalDF = finalDF.orderBy(finalDF.col("ID"));
        finalDF.show();

        WindowSpec window = Window.partitionBy("ID","ACT_IN").orderBy(finalDF.col("EFF_DT").desc()) ;

        finalDF = finalDF.withColumn("row_num", functions.row_number().over(window))
                .filter(new Column("row_num").equalTo(1)).drop("row_num") ;

        finalDF.show();


    }

    private static Dataset<Row> prepareMasterDF(){

        List<Row> list1= Arrays.asList(
                RowFactory.create(100,"Naresh","BBL","2024-10-10","9999-12-31","Y"),
                RowFactory.create(101,"Dashan","HYD","2024-10-10","9999-12-31","Y"),
                RowFactory.create(102,"Sharanaya","VIZAG","2024-10-10","9999-12-31","Y")

        );

        StructType structType1 = new StructType(
                new StructField[]{
                        new StructField("ID", DataTypes.IntegerType,false, Metadata.empty()),
                        new StructField("NAME", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("CITY", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("EFF_DT", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("END_DT", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("ACT_IN", DataTypes.StringType,false, Metadata.empty())
                }
        );

        Dataset<Row> sourceDF= sparkSession.createDataFrame(list1,structType1);
        return sourceDF;

    }

    private static Dataset<Row> prepareDeltaDF(){

        List<Row> list1= Arrays.asList(
                RowFactory.create(100,"Naresh","HYD"),
                RowFactory.create(101,"Dashan","HYD"),
                RowFactory.create(102,"Sharanaya","VIZAG"),
                RowFactory.create(103,"Kiyansh","VIZAG")

        );

        StructType structType1 = new StructType(
                new StructField[]{
                        new StructField("ID", DataTypes.IntegerType,false, Metadata.empty()),
                        new StructField("NAME", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("CITY", DataTypes.StringType,false, Metadata.empty())
                }
        );

        Dataset<Row> targetDF= sparkSession.createDataFrame(list1,structType1);
        return targetDF;

    }
}
