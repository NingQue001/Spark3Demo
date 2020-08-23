package structuredStreamingJDemo;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import java.sql.Date;
import java.util.concurrent.TimeoutException;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.Constants.STRING;

/**
 * The internet of things, 万物互联
 *
 * 订阅kafka
 *
 */
public class IotDevice {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // SparkSession在sql package
        SparkSession spark = SparkSession
                .builder()
                .appName("IotDevice")
                .master("local")
                .getOrCreate();

        /**
         * 订阅Kafka原始DataFrame打印
         *
         * +----+--------------------+---------+---------+------+--------------------+-------------+
         * | key|               value|    topic|partition|offset|           timestamp|timestampType|
         * +----+--------------------+---------+---------+------+--------------------+-------------+
         * |null|[66 75 6E 6B 20 7...|IotDevice|        1|     0|2020-08-23 11:45:...|            0|
         * +----+--------------------+---------+---------+------+--------------------+-------------+
         */
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "node03:9092,node02:9092,node01:9092")
                .option("subscribe", "IotDevice")
                .load();

        Dataset<DeviceData> dd = df
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING())
                .map(new MapFunction<String, DeviceData>() {
                    @Override
                    public DeviceData call(String value) throws Exception {
                        String[] valueList = value.split(",");
                        DeviceData deviceData = new DeviceData();
                        deviceData.setDevice(valueList[0]);
                        deviceData.setDeviceType(valueList[1]);
                        deviceData.setSignal(Double.parseDouble(valueList[2]));
                        deviceData.setTime(Date.valueOf(valueList[3]));
                        return deviceData;
                    }
                }, Encoders.bean(DeviceData.class));

        StreamingQuery query = dd.writeStream()
                .outputMode("append") // complete append update
                .format("console")
                .start();

        query.awaitTermination();
    }
}

