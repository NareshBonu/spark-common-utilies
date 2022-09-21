package impl;

import exception.GKCStoreException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import util.LoadPipelineDetails;
import util.SparkCommons;

import java.io.IOException;

public class EnrichProductRef {

    private  static Logger logger = Logger.getLogger(EnrichProductRef.class);
    private static SparkSession sparkSession;
    private static Dataset<Row> validCurrentDayData;
    private static Dataset<Row> productRef;
    private static Dataset<Row>  saleAmountEnrichment;
    private static String ingDay ="TODAY";

    public static void main(String args[])   throws IOException, GKCStoreException {


        String runDay = ingDay.equalsIgnoreCase("TODAY") ? SparkCommons.getCurrentDate("ddMMyyyy") : SparkCommons.getNextDayToCurrentDate("ddMMyyyy");
        logger.info("Run Day :"+runDay);

        LoadPipelineDetails.loadPipelineDetails();

        //Valid CurrentDay Path
        String outputFilePathValid = LoadPipelineDetails.outputFilePath+"Valid\\";
        outputFilePathValid        = outputFilePathValid+"ValidData_"+runDay;

        String productRefPath = LoadPipelineDetails.productFilePath;
        String saleAmountEnrichmentOutputPath = LoadPipelineDetails.saleAmountEnrichmentOutputPath;
        saleAmountEnrichmentOutputPath = saleAmountEnrichmentOutputPath+ "saleAmountEnrichment_"+runDay;
        //Creating SparkSession
        sparkSession =SparkCommons.createSparkSession("","");

        //Read currentDay valid date  File ..
        validCurrentDayData= SparkCommons.sparkReader(sparkSession,outputFilePathValid,"|","CSV","Sales");
        validCurrentDayData.createOrReplaceTempView("validCurrentDayData");
        validCurrentDayData.show();

        productRef= SparkCommons.sparkReader(sparkSession,productRefPath,"|","CSV","Products");
        productRef.createOrReplaceTempView("productRef");
        productRef.show();

        saleAmountEnrichment = sparkSession.sql("select V.Sale_ID,V.Product_ID,P.Product_Name,P.Product_Price," +
                "V.Quantity_Sold,V.Vendor_ID,V.Sale_Date," +
                "P.Product_Price * V.Quantity_Sold as Sale_Amount,V.Sale_Currency " +
                "from validCurrentDayData V INNER JOIN productRef P " +
                "ON V.Product_ID = P.Product_ID");

        saleAmountEnrichment.show();

        //Write Sales Amount Enriched
        SparkCommons.sparkWriter(saleAmountEnrichment, saleAmountEnrichmentOutputPath, "|", "csv");

    }
}
