package SCD2;

import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class SCD2Implemention {
    private static SparkSession sparkSession;
    public static void main(String[] args){


        sparkSession = SparkCommons.createSparkSession("", "");
        Dataset<Row> masterDF= prepareMasterDF();
        Dataset<Row> deltaDF= prepareDeltaDF();

        Column join_Condition = masterDF.col("ID").equalTo(deltaDF.col("ID")) ;
        Dataset<Row> joinedDF = deltaDF.join(masterDF, join_Condition, "left_outer");
        joinedDF.show();

        //Filter for New Record
        Dataset<Row> new_records_DF  = joinedDF.filter(masterDF.col("ID").isNull());
        new_records_DF = new_records_DF.select(deltaDF.col("ID"),deltaDF.col("NAME"),deltaDF.col("CITY"));

        //Filter for Updated Record
        Dataset<Row> updated_records_DF = joinedDF.filter(masterDF.col("CITY").notEqual(deltaDF.col("CITY")));
        updated_records_DF = updated_records_DF.select(deltaDF.col("ID"),deltaDF.col("NAME"),deltaDF.col("CITY"));

        //Union NEW and Updated Records
        Dataset<Row> new_and_updated_recordsDF = new_records_DF.union(updated_records_DF) ;

        //Add Audit Columns to New and Updated records
        new_and_updated_recordsDF =  new_and_updated_recordsDF
                .withColumn("EFF_DT", functions.date_format(functions.current_date(),"yyyy-MM-dd"))
                .withColumn("END_DT", functions.lit("9999-12-31"))
                .withColumn("ACT_IN", functions.lit("Y")) ;
        log.info("New and Updated Records---------------------");
        new_and_updated_recordsDF.show();



        //Mark old records as inactive
        Dataset<Row>    inactive_recordsDF = masterDF.join(new_and_updated_recordsDF.select("id"), "ID", "inner");
        inactive_recordsDF =inactive_recordsDF
                .withColumn("END_DT", functions.date_format(functions.current_date(),"yyyy-MM-dd"))
                .withColumn("ACT_IN", functions.lit("N"));
        log.info("INACTIVE RECORDS---------------------");
        inactive_recordsDF.show();

        //Remove old record from Master DF which is going to updated
        masterDF = masterDF.join(updated_records_DF,"ID","leftanti") ;

        // Master DF + New Records + Updated Records + Inactive Records
        Dataset<Row>  all_records_DF = masterDF.union(new_and_updated_recordsDF).union(inactive_recordsDF);
        all_records_DF = all_records_DF.orderBy(all_records_DF.col("ID"));

        log.info("FINAL Records---------------------");
        all_records_DF.show();

        //Remove old updated record from MASTER DF
       /* WindowSpec window = Window.partitionBy("ID","ACT_IN").orderBy(all_records_DF.col("EFF_DT").desc()) ;

        Dataset<Row> finalDF = all_records_DF.withColumn("row_num", functions.row_number().over(window))
                .filter(new Column("row_num").equalTo(1)).drop("row_num") ;

        finalDF.show();*/


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
