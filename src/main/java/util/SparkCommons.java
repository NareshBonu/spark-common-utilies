package util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import exception.GKCStoreException;
import lombok.extern.slf4j.Slf4j;

import model.FieldMetadata;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;



import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;



@Slf4j
public class SparkCommons {
    private  static Logger logger = Logger.getLogger(SparkCommons.class);

    public static SparkSession createSparkSession(String appName,String master){
        Logger.getLogger("org.apache").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder()
                .appName("spark-common-utilies")
                .master("local[*]")
               // .enableHiveSupport()
                .getOrCreate();
        logger.info("Spark Session created successfully.");


        return sparkSession;

    }

    public static StructType buildSparkSchema(String fileType) throws IOException, GKCStoreException {
        String jsonSchemaFile =null;
        StructType structType =  null;
        List<StructField> structField = new ArrayList<>();
        if (fileType.equalsIgnoreCase("Sales")) {
            jsonSchemaFile = SparkCommons.class.getClassLoader().getResource("SalesLanding_Schema.json").getFile();
        }
        else if(fileType.equalsIgnoreCase("Products")){
            jsonSchemaFile = SparkCommons.class.getClassLoader().getResource("Products_Schema.json").getFile();
        }
        List<FieldMetadata> fieldMetadata = getFieldMetadataFromJSONFile(jsonSchemaFile);


        for (FieldMetadata field : fieldMetadata){
            String fieldDataType = field.fieldDataType;
            String fieldName = field.fieldName;
            boolean fieldNullability = field.fieldNullability;
            if (fieldDataType.equalsIgnoreCase("String"))
                structField.add(DataTypes.createStructField(fieldName,DataTypes.StringType,fieldNullability));
            else if (fieldDataType.equalsIgnoreCase("Integer"))
                structField.add(DataTypes.createStructField(fieldName,DataTypes.IntegerType,fieldNullability));
            else if (fieldDataType.equalsIgnoreCase("Long"))
                structField.add(DataTypes.createStructField(fieldName,DataTypes.LongType,fieldNullability));
            else if (fieldDataType.equalsIgnoreCase("Double"))
                structField.add(DataTypes.createStructField(fieldName,DataTypes.DoubleType,fieldNullability));
            else if (fieldDataType.equalsIgnoreCase("Decimal"))
                structField.add(DataTypes.createStructField(fieldName,DataTypes.createDecimalType(),fieldNullability));
            else if (fieldDataType.equalsIgnoreCase("Date"))
                structField.add(DataTypes.createStructField(fieldName,DataTypes.DateType,fieldNullability));
            else if (fieldDataType.equalsIgnoreCase("Timestamp"))
                structField.add(DataTypes.createStructField(fieldName,DataTypes.TimestampType,fieldNullability));
            else{
                logger.error("fieldDataType:"+fieldDataType+" is not available.");
            }
        }
        structType = DataTypes.createStructType(structField);
        return structType  ;
    }

    public static Dataset<Row> sparkReader(SparkSession sparkSession, String filePath, String delimiter,String format,String fileName ) throws GKCStoreException {
        Dataset<Row> readDataset = null;
        try {
            readDataset = sparkSession.read().format("csv")
                    .schema(buildSparkSchema(fileName))
                    .option("delimiter", delimiter)
                    .load(filePath);
        }
        catch(Exception exp){
            throw new GKCStoreException(exp.getMessage(),exp.getCause());

        }
        return readDataset;
    }

    public static boolean sparkWriter(Dataset<Row> writeDataset, String filePath, String delimiter,String format ) throws GKCStoreException {
        boolean writeStatus = false;
        try {
            writeDataset.write().saveAsTable("customer");
            writeDataset.write().format("csv")
                    .option("delimiter", delimiter)
                    .option("header",false)
                    .mode(SaveMode.Overwrite)
                    .save(filePath);
            writeStatus = true;
            logger.info("Write to File Completed.");
        }
        catch(Exception exp){
            throw new GKCStoreException(exp.getMessage(),exp.getCause());

        }
        return writeStatus;
    }

    public static List<FieldMetadata> getFieldMetadataFromJSONFile(String jsonSchemaFile) throws IOException, GKCStoreException {
        List<FieldMetadata> fieldMetadata = null;
        try {
            logger.info("jsonSchemaFile:" + jsonSchemaFile);
            ObjectMapper objectMapper = new ObjectMapper();
            fieldMetadata = objectMapper.readValue(new File(jsonSchemaFile)
                    , new TypeReference<List<FieldMetadata>>() {
                    });

            logger.info("number of elements:" + fieldMetadata.size());

        }
        catch (Exception exp){
            throw new GKCStoreException(exp.getMessage(),exp.getCause());
        }
        return fieldMetadata;
    }

    public static void main(String args[]) throws IOException, GKCStoreException {

        String lookupFile = SparkCommons.class.getClassLoader().getResource("Lookup.dat").getFile();
        SparkSession sparkSession = createSparkSession("","");
        Dataset<Row> df3= sparkSession.read().format("csv")
                .option("delimiter","|")
                .option("header",true)
                .load("file://"+lookupFile);

        LongAccumulator longAccumulator=sparkSession.sparkContext().longAccumulator("recordCount");
        df3.foreach((ForeachFunction<Row>) row -> longAccumulator.add(1));


        System.out.println("rc:"+longAccumulator.value());
        //df3.show();
        //Dataset<Row> df4 = df3.filter(df3.col("FileName").equalTo("Sample.dat6")).select("Row_Count");
        //df4.show();

        JavaPairRDD<String, Integer> lookupPairRDD = df3.toJavaRDD().mapToPair(
                new PairFunction<Row, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(row.get(0).toString(),
                                Integer.valueOf(row.get(1).toString()));
                    }
                }
        );
        System.out.println(lookupPairRDD.lookup("Sample.dat6").get(0));
        System.out.println(lookupPairRDD.lookup("Sample.dat6").size());

        System.exit(0);


        List<Row> list= Arrays.asList(
               RowFactory.create("Naresh","M"),
               RowFactory.create("Sharanya","F"),
               RowFactory.create("Sharanya","")
       );

       StructType structType = new StructType(
         new StructField[]{
                 new StructField("Name",DataTypes.StringType,false,Metadata.empty()),
                 new StructField("Gender",DataTypes.StringType,false,Metadata.empty())
         }
       );



        Dataset<Row> df= sparkSession.createDataFrame(list,structType);
        df.show();

        Dataset<Row> df1 =df.withColumn("Gender", org.apache.spark.sql.functions
                .when(df.col("Gender").equalTo("M") , "Male")
                .when(df.col("Gender").equalTo("F") , "FeMale")
                .otherwise("Unknown")
        );
        df1= df1.withColumn("Current_timestamp",functions.lit(functions.current_timestamp()));
        df1.show();
        df1.printSchema();

        String sampleFile = SparkCommons.class.getClassLoader().getResource("Sample.dat").getFile();
        Dataset<Row> df2= sparkSession.read().format("csv")
                .option("ignoreTrailingWhiteSpace ",false)
                .load("file://"+sampleFile);
        df2.show();


    }

    public static String getCurrentDate(String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        String currentDate = simpleDateFormat.format(new Date().getTime());;
        return currentDate;
    }   

    public static String getNextDayToCurrentDate(String format){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
        String nextDay = simpleDateFormat.format(DateUtils.addDays(new Date(),1));

        return nextDay;
    }
}

