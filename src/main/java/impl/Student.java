package impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class Student {
    private static final Logger logger= Logger.getLogger(Student.class);
    private static Dataset<Row> inputDataset ;
    private static SparkSession sparkSession;

    public static void main(String[] args){

        Logger.getLogger("org.apache").setLevel(Level.OFF);

        String inputFilePath ="file:\\C:\\Naresh-AWS\\Student-Data.txt";

        StructType structType = new StructType(
                new StructField[]{
                        new StructField("SID", DataTypes.IntegerType,false, Metadata.empty()),
                        new StructField("SNAME", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("FL", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("SL", DataTypes.StringType,false, Metadata.empty()),
                        new StructField("SCORE", DataTypes.IntegerType,false, Metadata.empty())
                }
                );
        System.setProperty("hadoop.home.dir","C:\\SPARK\\hadoop_win\\bin\\winutils.exe");
        sparkSession=SparkSession.builder().master("local").appName("StudentData")
                //.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
              //  .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
               // .config("com.amazonaws.services.s3a.enableV4", "true")
                //.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
                //.config("spark.hadoop.fs.s3a.awsAccessKeyId","AKIA3AWQPJTZIIUGY2GL")
                //.config("spark.hadoop.fs.s3a.awsSecretAccessKey", "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")
                .getOrCreate();

     //   sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "AKIA3AWQPJTZIIUGY2GL");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "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");
        sparkSession.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com");

        Dataset<Row> allUsersData = sparkSession.read().format("csv")
                                    .option("delimiter", "|")
                                    .option("inferSchema","false")
                                    .schema(buildUsersSchema())
                                    .load("file:\\C:\\Users\\Lenovo\\Downloads\\tickitdb\\allusers_pipe.txt");

        allUsersData.show(false);
        logger.info("Record count:"+allUsersData.count());

        Dataset<Row> allUsersData1 = deleteColumns(allUsersData,new String[2]);
        allUsersData1.show(false);

        allUsersData1.printSchema();

        Dataset<Row> allUsersData2 = removeNullData(allUsersData1,new String[2]);
        allUsersData2.show(false);

        logger.info("Filtered Data record count:"+allUsersData2.count());
        System.exit(0);

        allUsersData2.write().mode(SaveMode.Overwrite)
                //.option("delimiter","|")
                .parquet("s3a://allusers-n663791/allusersdata/");
        logger.info("User data loaded to AWS S3 successfully.");

        allUsersData2.write().mode(SaveMode.Overwrite)
                .option("header","true")
                .csv("file:\\C:\\saranya-excel-prac\\");
        logger.info("User data loaded to local drive successfully.");

        System.exit(0);




        inputDataset = sparkSession.read().format("csv")
                                    .option("delimiter","|")
                                    .schema(structType)
                                    .load(inputFilePath);

        inputDataset.show();

        //Find out number of students chosen Telugu as FL
        Dataset<Row> inputDataset1  =inputDataset.filter(inputDataset.col("FL").equalTo("Telugu"));
        inputDataset1.show();
        logger.info("Number of students chosen Telugu as FL="+inputDataset1.count());

        //Find out number of students chosen Hindi as FL
        Dataset<Row> inputDataset2  =inputDataset.filter(inputDataset.col("FL").equalTo("Hindi"));
        inputDataset2.show();
        logger.info("Number of students chosen Hindi as FL="+inputDataset2.count());

        //Find out number of students scored more than 500
        Dataset<Row> inputDataset3  =inputDataset.filter(inputDataset.col("SCORE").gt(500));
        inputDataset3.show();
        logger.info("Number of students scored more than 500="+inputDataset3.count());

        //Sort the students by the score in descending order
        Dataset<Row> inputDataset4  =inputDataset.orderBy(inputDataset.col("SCORE").desc());
        inputDataset4.show();

        inputDataset4.write().mode(SaveMode.Overwrite)
            .option("delimiter","|")
            //.format("csv")
            .csv("s3a://student9thclass/naresh/");

        logger.info("Student data loaded into S3 bucket successfully.");

        Dataset<Row> inputDataset5 = sparkSession.read()
                .option("delimiter","|")
                .schema(structType)
                .csv("s3a://student9thclass/naresh/");
        inputDataset5.show();

        inputDataset5.write().format("jdbc")
                .option("url", "jdbc:mysql://student.cg4ryhkkylxa.ap-south-1.rds.amazonaws.com:3306/student")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "student.student")
                .option("user", "admin")
                .option("password", "Change00")
                .mode(SaveMode.Overwrite)
                .save();

        logger.info("Student data loaded into AWS RDS successfully.");
    }

private static StructType buildUsersSchema(){

        StructType structType = new StructType(
            new StructField[]{
                    new StructField("SNO",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("PASSPORT",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("FNAME",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("MNAME",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("LNAME",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("STATE",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("EMAILID",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("PHONE",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("US_CITIZEN",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("HAS_SSN",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("EMPLOYEE_STATUS",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("DELETE_COLUMN1",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("DELETE_COLUMN2",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("DELETE_COLUMN3",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("DELETE_COLUMN4",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("DELETE_COLUMN5",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("DELETE_COLUMN6",DataTypes.StringType,true,Metadata.empty()),
                    new StructField("DELETE_COLUMN7",DataTypes.StringType,true,Metadata.empty()),
            }

        );

        return  structType;
}

private static Dataset<Row> deleteColumns(Dataset<Row> inputDataset, String[] columnList){

        List<String> columnList1 = Arrays.asList("SNO","DELETE_COLUMN1","DELETE_COLUMN2","DELETE_COLUMN3","DELETE_COLUMN4",
                "DELETE_COLUMN5","DELETE_COLUMN6","DELETE_COLUMN7");
    String columnListStr = StringUtils.join(columnList1,",");
         return inputDataset.drop("SNO","DELETE_COLUMN1","DELETE_COLUMN1","DELETE_COLUMN2","DELETE_COLUMN3",
                 "DELETE_COLUMN4","DELETE_COLUMN5","DELETE_COLUMN6","DELETE_COLUMN7");
   }

   private static Dataset<Row> removeNullData(Dataset<Row> inputDataset, String[] columnList){

        return inputDataset.filter(inputDataset.col("HAS_SSN").isNotNull()
                .and(inputDataset.col("EMPLOYEE_STATUS").isNotNull()));
   }
}

