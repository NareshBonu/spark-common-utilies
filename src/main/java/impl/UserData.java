package impl;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class UserData {

    private static final Logger logger= Logger.getLogger(UserData.class);
    private static SparkSession sparkSession;
    public static Dataset<Row> inputDataset;
    public static Dataset<Row> outputDataset;

    public static void main (String args[]) throws IOException {

        String josnString  = FileUtils.readFileToString(new File("./src/main/resource/schema.json"));
        StructType structType = (StructType) StructType.fromJson(josnString);


        //String inputFilePath = "file:\\C:\\saranya-excel-data\\users-data.csv";
        String inputFilePath = "file:\\C:\\SPARK\\banktrans.csv";

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        //Create SparkSession
        sparkSession = SparkSession.builder().appName("Test").master("local[*]").getOrCreate();
        inputDataset = sparkSession.read().format("csv")
                        .option("header",true)
                        .load(inputFilePath);

        String[] droppedCols = new String[2];
        droppedCols[0] = "DOT";
        droppedCols[1] = "CHQNO";
        inputDataset = inputDataset.drop(droppedCols)
                .withColumn("AccountNo",inputDataset.col("AccountNo").cast(DataTypes.LongType));
        inputDataset.show();

        List<String> columnList = Stream.of("WITHDRAWALAMT","DEPOSITAMT","BALANCEAMT").collect(Collectors.toList());
        List<String> dateColumnList = Stream.of("DATE","VALUEDATE").collect(Collectors.toList());

        columnList.forEach(colName -> cleanAndCastAmountCols(colName));
        dateColumnList.forEach(colName -> cleanAndCastDateCols(colName));
/*
        inputDataset.coalesce(1).write().option("header","true").mode(SaveMode.Overwrite).format("csv")
                .save("file:\\C:\\SPARK\\banktrans1.csv");*/

        logger.info("Record Count:"+inputDataset.count());
        /*inputDataset =inputDataset.withColumn("WITHDRAWALAMT",
                functions.when(withAmt.isNull(),functions.lit("0"))
                        .otherwise(
                                functions.regexp_replace(withAmt,",","")
                        )).withColumn("DEPOSITAMT",
                functions.when(depositAmt.isNull(),functions.lit("0"))
                        .otherwise(
                                functions.regexp_replace(depositAmt,",","")
                        )).withColumn("BALANCEAMT",
                functions.when(balAmt.isNull(),functions.lit("0"))
                        .otherwise(
                                functions.regexp_replace(balAmt,",","")
                        ));
*/
       
        inputDataset.show();
        inputDataset.printSchema();

        System.exit(0);

       logger.info("Record count:"+inputDataset.count());
       inputDataset = inputDataset
                .withColumn("US_CITIZEN", inputDataset.col("US_CITIZEN").cast(DataTypes.BooleanType))
                .withColumn("HAS_SSN", inputDataset.col("HAS_SSN").cast(DataTypes.BooleanType))
                .withColumn("EMPLOYEE_STATUS", inputDataset.col("EMPLOYEE_STATUS").cast(DataTypes.BooleanType));

       inputDataset.printSchema();

        inputDataset = inputDataset.filter(
                inputDataset.col("US_CITIZEN").equalTo(true)
                .and(inputDataset.col("HAS_SSN").equalTo(true))
                .and(inputDataset.col("EMPLOYEE_STATUS").equalTo(true))
        );

        logger.info("Record count after filtering:"+inputDataset.count());

        inputDataset = inputDataset.withColumn("PHONE" ,
                functions.lit(inputDataset.col("PHONE").toString().replace("\\)",""))
               // functions.regexp_replace(inputDataset.col("PHONE"),"\\)","")
                        //.and(functions.regexp_replace(inputDataset.col("PHONE"),"\\(",""))
                        //.and(functions.regexp_replace(inputDataset.col("PHONE"),"-",""))
                        //.and(functions.regexp_replace(inputDataset.col("PHONE")," ",""))
        );
        inputDataset.show();




    }
    
    private static Dataset<Row> cleanAndCastAmountCols(String ColName){


        Column ColName1 = inputDataset.col(ColName);
        inputDataset =inputDataset.withColumn(ColName,
                functions.when(ColName1.isNotNull(),ColName1)
                        .otherwise(functions.lit("0"))
                        .cast(DataTypes.DoubleType)
                        );
       // inputDataset =inputDataset.withColumn(ColName,functions.regexp_replace(ColName1,".00",""));

        return inputDataset;
    }

    private static Dataset<Row> cleanAndCastDateCols(String ColName){


        Column ColName1 = inputDataset.col(ColName);
        inputDataset =inputDataset.withColumn(ColName,functions.to_date(ColName1,"dd-MMM-yy"));

        return inputDataset;
    }


}
