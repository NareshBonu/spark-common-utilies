package util;


import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

public class LoadPipelineDetails {
    private static final Logger logger = Logger.getLogger("LoadPipelineDetails.class");
    public static String inputFilePath;
    public static String outputFilePath;
    public static String productFilePath;
    public static String saleAmountEnrichmentOutputPath;
    public static void loadPipelineDetails() throws IOException {
        String pipelineDetailsFilePath= LoadPipelineDetails.class.getClassLoader().getResource("PipelineDetails").getFile();
        Properties properties = new Properties();
        properties.load(new FileReader(pipelineDetailsFilePath));
        inputFilePath = properties.getProperty("inputPath");
        outputFilePath=properties.getProperty("outputPath");
        productFilePath=properties.getProperty("ProductPath");
        saleAmountEnrichmentOutputPath=properties.getProperty("SaleAmountEnrichmentOutputPath");

    }

    public static void main(String[] args) throws IOException {
        loadPipelineDetails();
        String inputFilePath = LoadPipelineDetails.inputFilePath;
        logger.info("inputFilePath:"+inputFilePath);
    }

}
