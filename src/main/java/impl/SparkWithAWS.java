package impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.util.Arrays;
import java.util.List;


public class SparkWithAWS {

    private  static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SparkWithAWS.class);
    private static SparkSession sparkSession;
    private static Dataset<Row> inputDataset;



    public static void main(String args[]){

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        List<Row> inputData = Arrays.asList(
                RowFactory.create("Naresh",123,"22-06-1983"),
                RowFactory.create("Naresh",123,"22-06-1983"),
                RowFactory.create("Naresh",123,"22-06-1983")
        );

        StructType structType = new StructType(
          new StructField[]{
                  new StructField("Name", DataTypes.StringType,false, Metadata.empty()),
                  new StructField("ID", DataTypes.IntegerType,false, Metadata.empty()),
                  new StructField("DOB", DataTypes.StringType,false, Metadata.empty())
          }
        );

        sparkSession = SparkSession.builder().appName("SparkWithAWS").master("local").getOrCreate();
        inputDataset = sparkSession.createDataFrame(inputData,structType);
        inputDataset.show();

        Column dob1 = functions.to_date(inputDataset.col("DOB"),"dd-MM-yyyy");
        inputDataset= inputDataset.withColumn("DOB1", dob1);
        inputDataset.show();




        Dataset<Row> inputDataset1 = inputDataset.distinct();

        Column colExpr= inputDataset.col("ID").equalTo(inputDataset1.col("ID"));
        inputDataset.join(inputDataset1,colExpr,"leftsemi").show();

        int numPart = inputDataset.rdd().getNumPartitions();
       logger.info("numPart:"+numPart);

    }
}

