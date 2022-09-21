package sparkstreaming;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

@Slf4j
public class ConsumeFromKafkaTopic {

    private static final String BOOTSTRAP_SERVER ="localhost:9092";
    private static final String TOPIC_NAME ="first-topic";
    private static final String GROUP_ID ="KAFKA-GROUP";
    private static SparkSession sparkSession;
    private static Dataset<Row> inputDataset;

    public static void main(String args[]) throws StreamingQueryException {

        sparkSession = SparkSession.builder().appName("UDF Example").master("local[*]").getOrCreate();
        inputDataset = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                .option("subscribe", TOPIC_NAME)
                .option("startingOffsets", "latest") // earliest=From starting , latest = last meessage
                .load()
                ;
        inputDataset.printSchema();

        inputDataset.writeStream()
                .outputMode("append")
                .format("console")
                .start().awaitTermination();

        //inputDataset.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show();


        /*inputDataset.writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .option("truncate", false)
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start()
                .awaitTermination();*/




    }

}
