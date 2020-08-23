package structuredStreamingJDemo;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class JavaStructuredNetworkWordCount {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // SparkSession在sql package
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredNetworkWordCount")
                .master("local")
                .getOrCreate();

        // transform stream to dataset
        Dataset<Row> lines = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        // split lines into words
        Dataset<String> words = lines
                .as(Encoders.STRING())
                // 在scala中，这是哪种操作？
                .flatMap((FlatMapFunction<String, String>) x ->
                        Arrays.asList(x.split(" ")).iterator()
                , Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordcounts = words.groupBy("value").count();

        StreamingQuery query = wordcounts.writeStream()
                .outputMode("complete") // complete append update
                .format("console")
                .start();

        query.awaitTermination();
    }
}
