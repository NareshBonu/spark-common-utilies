package impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import exception.GKCStoreException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.json4s.jackson.Json;
import scala.Tuple2;
import util.SparkCommons;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Slf4j
public class Test {

    private static SparkSession sparkSession;

    public static void main(String args[]) throws IOException, GKCStoreException {


        String samplePath = "/Users/nareshbonu/Documents/Naresh/SPARK/Data/File2.txt";
        String samplePath1 = "/Users/nareshbonu/Documents/Naresh/SPARK/Data/File2_1.txt";

        String jsonFile="/Users/nareshbonu/Documents/Naresh/SPARK/Data/schema.json";
        String custFile="Users/nareshbonu/Documents/Naresh/SPARK/Data/Cust_data.txt";

        FileReader fileReader=new FileReader(jsonFile);
        JsonParser jsonParser = new JsonParser();
        String jsonString= jsonParser.parse(fileReader).toString();



        StructType structType1= (StructType) DataType.fromJson(jsonString);


        StructType schema= new StructType(
          new StructField[]
                  {
                   new StructField("ID",DataTypes.IntegerType,false,Metadata.empty())   ,
                          new StructField("Name",DataTypes.StringType,true,Metadata.empty())   ,
                          new StructField("City",DataTypes.StringType,true,Metadata.empty())
                  }
        );

        sparkSession = SparkCommons.createSparkSession("", "");
        Dataset<Row> inputDataset = sparkSession.read().schema(structType1).option("delimiter","|").csv(samplePath);
        inputDataset.show();
        Dataset<Row> inputDataset1 = sparkSession.read().schema(structType1).option("delimiter","|").csv(samplePath1);
        inputDataset1.show();

        inputDataset.createOrReplaceTempView("source_table");
        inputDataset1.createOrReplaceTempView("target_table");


        String query1 = "select * from source_table minus select * from target_table";
        Dataset<Row> dataset1 = sparkSession.sql(query1);
        dataset1.show();
        System.exit(1);



        String query = "Select SeqID, CustName, CustCity from source_table A Where NOT EXISTS( select 1 from target_table B Where A.SeqID = B.SeqID and A.CustName = B.CustName and A.CustCity = B.CustCity )";

        Dataset<Row> dataset2 = sparkSession.sql(query);
        dataset2.show();
        System.exit(1);

        Dataset<Row> missingRecordsFromSource = sparkSession.sql("select * from source_table except select * from target_table");
        missingRecordsFromSource = missingRecordsFromSource.withColumn("MISSING_FROM",functions.lit("TARGET"));

        Dataset<Row> missingRecordsFromTarget = sparkSession.sql("select * from target_table except select * from source_table");
        missingRecordsFromTarget = missingRecordsFromTarget.withColumn("MISSING_FROM",functions.lit("SOURCE"));


        Dataset<Row> finalDataset = missingRecordsFromSource.union(missingRecordsFromTarget);
        finalDataset.show();


        System.exit(1);



        List<Row> list= Arrays.asList(
                RowFactory.create("Naresh","2020-07-09","2020-07-09 15:32:16.023","2020-07-09 15:32:16","20180404112322.123","20180404112300.000012","2018-04-04-11.23.22.123","2021/03/18","20210318"),
                RowFactory.create("Sharanya","2020-08-09","2020-07-09 15:32:16.406","2020-07-09 15:32:16","20180404112323.120","20180405112300.000012","2018-04-04-11.23.22.123","2021/03/18","20210318"),
                RowFactory.create("Sharanya","2020-09-09","2020-07-09 00:00:00.001","2020-07-09 15:32:10","20180404112323.124","20180406112300.000012","2018-04-04-11.23.22.123","2021/03/18","20210318")
        );

        StructType structType = new StructType(
                new StructField[]{
                        new StructField("Name", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("DOB",DataTypes.StringType,false,Metadata.empty()),
                        new StructField("TS1",DataTypes.StringType,false,Metadata.empty()),
                        new StructField("TS2",DataTypes.StringType,false,Metadata.empty()),
                        new StructField("TS3",DataTypes.StringType,false,Metadata.empty()),
                        new StructField("TS4",DataTypes.StringType,false,Metadata.empty()),
                        new StructField("TS5",DataTypes.StringType,false,Metadata.empty()),
                        new StructField("DOB1",DataTypes.StringType,false,Metadata.empty()),
                        new StructField("DOB2",DataTypes.StringType,false,Metadata.empty())
                }
        );

        Column outputColValue;
        String tsFormat ;

        Dataset<Row> df= sparkSession.createDataFrame(list,structType);
        df.show(false);

        outputColValue= functions.to_date(df.col("DOB"),"yyyy-MM-dd");
        df=  df.withColumn("DOB", outputColValue);

        outputColValue= functions.to_date(df.col("DOB1"),"yyyy/MM/dd");
        df=  df.withColumn("DOB1", outputColValue);

        outputColValue= functions.to_date(df.col("DOB2"),"yyyyMMdd");
        df=  df.withColumn("DOB2", outputColValue);

        outputColValue = functions.to_utc_timestamp(df.col("TS1"),"yyyy-MM-dd HH:mm:ss.SSS");
        df=  df.withColumn("TS1", outputColValue);

        outputColValue = functions.to_timestamp(df.col("TS2"),"yyyy-MM-dd HH:mm:ss");
        df=  df.withColumn("TS2", outputColValue);

        tsFormat = "yyyyMMddHHmmss.SSS";
        outputColValue = castTimestamp( tsFormat,"TS3", df);
        df=  df.withColumn("TS3", outputColValue);

        tsFormat = "yyyyMMddHHmmss.SSSSSS";
        outputColValue = castTimestamp( tsFormat,"TS4", df);
        df=  df.withColumn("TS4", outputColValue);

        tsFormat = "yyyy-MM-dd-HH.mm.ss.SSS";
        outputColValue = castTimestamp( tsFormat,"TS5", df);
        df=  df.withColumn("TS5", outputColValue);

        df.show(false);
        df.printSchema();

        /*JavaRDD<String> rdd = sparkSession.sparkContext().textFile(samplePath, 1).toJavaRDD();
        JavaRDD<String> rdd1 =rdd.flatMap(row -> Arrays.asList(row.split(" ")).iterator());
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(row -> new Tuple2<>(row,1));
        System.out.println(rdd2.countByKey());
        JavaPairRDD<String, Integer> rdd3 = rdd2.reduceByKey( (v1,v2) ->(v1+v2) );
        System.out.println(rdd3.collectAsMap());

        Dataset<Row> inputDataset= sparkSession.read().format("csv").load(samplePath);
        inputDataset.show();
*/

    }

    public static Column castTimestamp(String tsFormat,String colName, Dataset<Row> inputDataset){
        Column colValue =null;


        String MSorNSFormat =StringUtils.substringAfterLast(tsFormat,".");
        int index = StringUtils.lastIndexOf(tsFormat,".")+2;
        Column ms1 = inputDataset.col(colName).substr(index, MSorNSFormat.length());
        Column ts1 = functions.to_timestamp(inputDataset.col(colName),tsFormat).cast("STRING");
        colValue= functions.concat_ws(".",ts1,ms1).cast("TIMESTAMP");
        return colValue;
    }
}


