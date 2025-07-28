package cdc;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import util.SparkCommons;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

@Slf4j
public class CDCWithSpark {

    private static SparkSession sparkSession;
    private static Dataset<Row> masterDataset;
    private static Dataset<Row> deltaDataset;
    private static Dataset<Row> finalDataset;
    private static String cdcType;
    private static String loadType;
    private static String keyColumns;

    private static final String MASTER_FILE ="/Users/nareshbonu/Documents/Naresh/SPARK/Data/CDC_DATA/DAY0";
    private static final String DELTA_FILE ="/Users/nareshbonu/Documents/Naresh/SPARK/Data/CDC_DATA/DAY1";
    private static final String CDC_OUTPUT ="/Users/nareshbonu/Documents/Naresh/SPARK/Data/CDC_DATA/CDC_OUTPUT";

    public static void main(String[] args) throws IOException, ParseException {

        Date date = new SimpleDateFormat("yyyyMMdd").parse("20220918");
        String year = new SimpleDateFormat("yyyy").format(date);

        cdcType="CDC1";
      // loadType="FULL";
        loadType="DELTA";
        keyColumns="ID";

        sparkSession = SparkCommons.createSparkSession("CDC with spark","local[*]");
        sparkSession.sparkContext().hadoopConfiguration().set("mapreduce.fileoutputcommiter.marksuccessful","false");


        //Implement CDC Logic...
        if ((cdcType =="CDC1") && (loadType == "FULL")){
            log.info("cdcType:{}||loadType:{}",cdcType,loadType);

            //read data from MASTER file...
            masterDataset = readDataFromFile(MASTER_FILE,"COMMA","false",true);
            long masterRecordCount=masterDataset.count();
            log.info("Record count of master file:{}",masterRecordCount);

            finalDataset = masterDataset
                            .withColumn("eff_start_date", functions.to_timestamp(functions.current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
                            .withColumn("eff_end_date",functions.lit("9999-12-31 23:59:00").cast(DataTypes.TimestampType));

        }
        else  if ((cdcType =="CDC1") && (loadType == "DELTA")){

            log.info("cdcType:{}||loadType:{}||keyColumns:{}",cdcType,loadType,keyColumns);

            //read master data from CDC_OUTPUT Folder
            masterDataset = readDataFromFile(CDC_OUTPUT,"COMMA","true",false);
            log.info("Number Of partitions:{}",masterDataset.rdd().getNumPartitions());
            log.info("Number of cores:{}",sparkSession.sparkContext().defaultParallelism());
            masterDataset=masterDataset.repartition(sparkSession.sparkContext().defaultParallelism());
            log.info("Number Of partitions:{}",masterDataset.rdd().getNumPartitions());
            System.exit(0);
            masterDataset.cache();
            masterDataset.createOrReplaceTempView("cdc_master");
            long masterRecordCount=masterDataset.count();

            //read data from DELTA File
            deltaDataset = readDataFromFile(DELTA_FILE,"COMMA","false",true);
            deltaDataset.cache();
            deltaDataset.createOrReplaceTempView("cdc_delta");
            long deltaRecordCount=deltaDataset.count();

            //common records between master and delta.. on all columns
            String commonRecords = "SELECT m.* FROM cdc_delta d  JOIN cdc_master m ON d.id = m.id and d.custName=m.custname and d.dob=m.dob";
            log.info("Query for commonRecords:{}",commonRecords);
            Dataset<Row> commonRecordsDataset = sparkSession.sql(commonRecords);
            commonRecordsDataset = commonRecordsDataset
                            .withColumn("eff_start_date",functions.col("eff_start_date").cast(DataTypes.TimestampType))
                    .withColumn("eff_end_date",functions.col("eff_end_date").cast(DataTypes.TimestampType));

            long commonsRecordCount=commonRecordsDataset.count();

            //Find out new records from delta file...
            String newRecords = "SELECT d.* FROM " +
                    "cdc_master m RIGHT OUTER JOIN cdc_delta d ON m.ID = d.ID " +
                    "WHERE m.ID IS NULL";
            log.info("Query for newRecords:{}",newRecords);
            Dataset<Row> newRecordsDataset = sparkSession.sql(newRecords);
            long newRecordCount=newRecordsDataset.count();

            //Find out update records from delta file...
            String updatedRecords = "SELECT T1.* FROM (" +
                    "(SELECT d.* FROM cdc_delta d  JOIN cdc_master m ON d.ID = m.id)  T1 " +
                    "LEFT OUTER JOIN " +
                    "(SELECT d.* FROM cdc_delta d  JOIN cdc_master m ON d.ID = m.ID AND d.custName=m.custname and d.dob=m.dob ) T2 " +
                    "ON T1.ID=T2.ID " +
                    ") WHERE T2.ID IS NULL";


            log.info("Query for updateRecords:{}",updatedRecords);
            Dataset<Row> updateRecordsDataset = sparkSession.sql(updatedRecords);
            long updatedRecordCount=updateRecordsDataset.count();

            String deletedRecords="SELECT m.id,m.custname,m.dob FROM cdc_master m LEFT JOIN cdc_delta d  ON d.ID = m.ID WHERE d.ID IS NULL";
            log.info("Query for deletedRecords:{}",deletedRecords);
            Dataset<Row> deletedRecordsDataset = sparkSession.sql(deletedRecords);
            long deletedRecordCount=deletedRecordsDataset.count();

            masterDataset.show();
            deltaDataset.show();
            newRecordsDataset.show();
            updateRecordsDataset.show();
            commonRecordsDataset.show();
            deletedRecordsDataset.show();

            log.info("mastertable:{}||deltaTable:{}||commonRecords:{}||newRecords:{}||updatedRecords:{}||deletedRecords:{}",
                    masterRecordCount,deltaRecordCount,commonsRecordCount,newRecordCount,updatedRecordCount,deletedRecordCount);
            if (deltaRecordCount == commonsRecordCount+newRecordCount+updatedRecordCount+deletedRecordCount){
                log.info("final count matching...");
            }else{
                log.error("final count not matching...");
            }

            writeDataset(deletedRecordsDataset);
            //union all newRecords and updatedRecords or deletedRecords.
            Dataset<Row> datasetMerge =newRecordsDataset.union(updateRecordsDataset);
            datasetMerge =datasetMerge
                            .withColumn("eff_start_date", functions.to_timestamp(functions.current_timestamp(),"yyyy-MM-dd HH:mm:ss"))
                            .withColumn("eff_end_date",functions.lit("9999-12-31 23:59:00").cast(DataTypes.TimestampType));
            finalDataset= datasetMerge.union(commonRecordsDataset);
            long finalRecordCount=finalDataset.count();
            log.info("Final Dataset Record Count:{}",finalRecordCount);

        }

        finalDataset.show(false);
        writeDataset(finalDataset);
    }

    private static StructType getMasterSchema() throws IOException {

        File file = Paths.get("src/main/resources/cdc_master_schema.json").toFile();
        String jsonString = FileUtils.readFileToString(file);

        return (StructType) DataType.fromJson(jsonString);

    }

    private static Dataset<Row> readDataFromFile(String filePth, String delimiter,String hasHeader, boolean withProvidedSchema) throws IOException {
        Dataset<Row> readerDataset = null;

        if (withProvidedSchema) {
            readerDataset = sparkSession.read()
                    .format("csv")
                    .option("delimiter", ",")
                    .option("header", hasHeader)
                    .schema(getMasterSchema())
                    .load(filePth);
        }else{
            readerDataset = sparkSession.read()
                    .format("csv")
                    .option("delimiter", ",")
                    .option("header", hasHeader)
                    .load(filePth);
        }

        return readerDataset;
    }

    private static void writeDataset(Dataset<Row> finalDataset) {
        // Write final output to master folder
        if (Objects.nonNull(finalDataset)) {
            finalDataset.repartition(1).write().mode(SaveMode.Overwrite)
                    .format("csv")
                    .option("header", "true")
                    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                    .save(CDC_OUTPUT);
            log.info("FINAL DATA LOADED INTO CDC-OUTPUT....");
        }
    }

    private static void deleteCRCFiles() throws IOException {
        Configuration conf= new Configuration(sparkSession.sparkContext().hadoopConfiguration());
        FileSystem fileSystem = FileSystem.get(conf);
        Path path= new Path(CDC_OUTPUT);
        if (fileSystem.exists(path)) {
            log.info("HDFS Path:{} exists.",CDC_OUTPUT);
            FileStatus[] fileStatus =fileSystem.listStatus(path);
            for (FileStatus fileStatus1 : fileStatus){
                log.info( "file name:{}",fileStatus1.getPath());
            }
        }
    }

}
