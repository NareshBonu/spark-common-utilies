package impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SparkUDFExample {

    private static final org.apache.log4j.Logger logger= Logger.getLogger(SparkUDFExample.class);
    private static SparkSession sparkSession;
    private static Dataset<Row> inputDataset;
    public static void main(String args[]){

        String custFile="/Users/nareshbonu/Documents/Naresh/SPARK/Data/Cust_data.txt";

        Logger.getLogger("org.apache").setLevel(Level.OFF);
        sparkSession = SparkSession.builder().appName("UDF Example").master("local[*]").getOrCreate();
        inputDataset = sparkSession.read().format("jdbc").options(getJDBCProperties()).load();
        Column colCity=inputDataset.col("city");
        Column colDOB=inputDataset.col("dob");
        Column colID=inputDataset.col("id");
        inputDataset=inputDataset.withColumn("city",
                functions.when(colCity.equalTo("hyd"),functions.lit("hyderabad"))
                .otherwise(functions.lit("visakhapatnam"))
        );

        inputDataset=inputDataset
                .withColumn("dob",functions.date_add(colDOB,2))
                .withColumn("id",colID.plus(1));

        inputDataset.show();

        inputDataset.write().format("jdbc").options(getJDBCPropertiesForWrite()).mode(SaveMode.Append).save();

        System.exit(1);

        inputDataset=sparkSession.read()
                .option("delimiter","|")
                .option("header",true)
                .format("csv")
                .load(custFile);
        inputDataset.show(false);
        Column allCols= inputDataset.col("*");
        Column col=inputDataset.col("Address-Contact");


        inputDataset=inputDataset.select(allCols,
                functions.split(col,"-").getItem(0).as("Address"),
                functions.split(col,"-").getItem(1).as("Phone"));
        inputDataset.show(false);
        Column col1=inputDataset.col("Address");
        Column col2=inputDataset.col("Phone");

       inputDataset=inputDataset.withColumn("Address",functions.when(col1.rlike("[^0-9]"),col1).otherwise(null))
                                .withColumn("Phone",functions.when(col.rlike("^[0-9]*$"),col).otherwise(col2)); //^[0-9]*$"

       inputDataset.show(false);

    }

    private static Map<String,String> getJDBCProperties(){
        Map<String,String> properties = new HashMap<>();
        properties.put("driver","com.mysql.jdbc.Driver");
        properties.put("url","jdbc:mysql://localhost:3306/n663791");
        properties.put("user","root");
        properties.put("password","Change00@");
        properties.put("dbtable","customer");

        return  properties;

    }

    private static Map<String,String> getJDBCPropertiesForWrite(){
        Map<String,String> properties = new HashMap<>();
        properties.put("driver","com.mysql.jdbc.Driver");
        properties.put("url","jdbc:mysql://localhost:3306/n663791");
        properties.put("user","root");
        properties.put("password","Change00@");
        properties.put("dbtable","customer_3");

        //properties.put("dbtable","customer");

        return  properties;

    }
}
