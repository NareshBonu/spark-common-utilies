package impl;

import exception.GKCStoreException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.LoadPipelineDetails;
import util.SparkCommons;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class DailyDataIngestion {
    private  static Logger logger = Logger.getLogger(DailyDataIngestion.class);
    private static SparkSession sparkSession;
    private static Dataset<Row> inputDataset;
    private static Dataset<Row> previousHoldDataset;
    private static Dataset<Row> updatedDataset;
    private static Dataset<Row> nonUpdatedDataset;
    private static String ingDay ;
    public static void main(String args[])   throws IOException, GKCStoreException {

        List<String> validInput = Arrays.asList("TODAY", "TOMORROW");


        if (args.length == 0){
            System.out.println("Input Value not passed.Exiting from Job Run!!!");
            logger.error("Parameter not passed.Exiting from Job Run!!!");
            System.exit(1);
        }
        else if(!validInput.contains(args[0]))
        {
            System.out.println("Invalid input value passed.Valid Input value is TODAY or TOMORROW..Exiting from Job Run!!!");
            logger.error("Invalid input value passed.Valid Input value is TODAY or TOMORROW..Exiting from Job Run!!!");
            System.exit(1);
        }
        else {
            ingDay = args[0];
            System.out.println("Valid Input value passed.");
            logger.error("Valid Input value passed:"+ingDay);
        }


        String runDay = ingDay.equalsIgnoreCase("TODAY") ? SparkCommons.getCurrentDate("ddMMyyyy") : SparkCommons.getNextDayToCurrentDate("ddMMyyyy");
        logger.info("Run Day :"+runDay);

        LoadPipelineDetails.loadPipelineDetails();
        String inputFilePath = LoadPipelineDetails.inputFilePath;
        inputFilePath = inputFilePath+runDay+"\\SalesDump.dat";

        //String outputFilePath = LoadPipelineDetails.outputFilePath;
        String outputFilePathValid = LoadPipelineDetails.outputFilePath+"Valid\\";
        outputFilePathValid        = outputFilePathValid+"ValidData_"+runDay;

        String outputFilePathHold = LoadPipelineDetails.outputFilePath+"Hold\\";
        outputFilePathHold        = outputFilePathHold+"HoldData_"+runDay;

        String previousOutputFilePathHold= LoadPipelineDetails.outputFilePath+"Hold\\"+"HoldData_"+SparkCommons.getCurrentDate("ddMMyyyy") ;


        //Creating SparkSession
        sparkSession =SparkCommons.createSparkSession("","");

        //Read input File ..
        Dataset<Row> inputDataset = SparkCommons.sparkReader(sparkSession,inputFilePath,"|","CSV","Sales");
        inputDataset.createOrReplaceTempView("TodayData");
        inputDataset.show();

        if ( ingDay.equalsIgnoreCase("TOMORROW"))  {
            previousHoldDataset = SparkCommons.sparkReader(sparkSession, previousOutputFilePathHold, "|", "CSV","Sales");
            previousHoldDataset.createOrReplaceTempView("PreviousDayHoldData");
            previousHoldDataset.show();
        }
        if ( ingDay.equalsIgnoreCase("TODAY") ) {
            //Filter Valid Data
            Dataset<Row> validInputDataset = inputDataset.filter(inputDataset.col("Quantity_Sold").isNotNull().and(
                    inputDataset.col("Vendor_ID").isNotNull()));
            validInputDataset.show();

            //Write Valid Data to Valid OutputFile Path
            SparkCommons.sparkWriter(validInputDataset, outputFilePathValid, "|", "csv");

            //Filter Hold Data
            Dataset<Row> inValidInputDataset = inputDataset.filter(inputDataset.col("Quantity_Sold").isNull().or(
                    inputDataset.col("Vendor_ID").isNull()));
            inValidInputDataset.show();

            //Write Hold data to Hold OutputFile Path
            SparkCommons.sparkWriter(inValidInputDataset, outputFilePathHold, "|", "csv");
        }
        else{
            updatedDataset = sparkSession.sql("select T.Sale_ID, T.Product_ID ," +
                    "CASE WHEN (T.Quantity_Sold IS NULL) THEN P.Quantity_Sold ELSE  T.Quantity_Sold END AS Quantity_Sold," +
                    "CASE WHEN (T.Vendor_ID is null) THEN P.Vendor_ID ELSE  T.Vendor_ID END AS Vendor_ID," +
                    "T.Sale_Date,T.Sale_Amount,T.Sale_Currency " +
                    "from TodayData T LEFT OUTER JOIN PreviousDayHoldData P " +
                    "ON T.Sale_ID = P.Sale_ID ");
            updatedDataset.show();

            //Filter Valid data
            Dataset<Row> validInputDataset = updatedDataset.filter(updatedDataset.col("Quantity_Sold").isNotNull().and(
                    updatedDataset.col("Vendor_ID").isNotNull()));
            validInputDataset.show();

            //Write Valid Data to Valid OutputFile Path
            SparkCommons.sparkWriter(validInputDataset, outputFilePathValid, "|", "csv");

            //Filter Hold Data
            Dataset<Row> inValidInputDataset = updatedDataset.filter(updatedDataset.col("Quantity_Sold").isNull().or(
                    updatedDataset.col("Vendor_ID").isNull()));
            inValidInputDataset.show();

            nonUpdatedDataset = sparkSession.sql("select P.* " +
                    "from TodayData T RIGHT OUTER JOIN PreviousDayHoldData P " +
                    "ON T.Sale_ID == P.Sale_ID where T.Sale_ID is NULL");
            nonUpdatedDataset.show();

            Dataset<Row> finalInValidInputDataset = inValidInputDataset.union(nonUpdatedDataset);
            finalInValidInputDataset.show();

            //Write Hold data to Hold OutputFile Path
            SparkCommons.sparkWriter(finalInValidInputDataset, outputFilePathHold, "|", "csv");

        }
    }
}
