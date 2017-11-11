package ru.bmstu.hadoop.lab4;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by madina on 11.11.17.
 */
public class SparkFlightsTable {
    public static void main(String[] args) throws Exception {
        SparkConf config = new SparkConf().setAppName("lab4");
        JavaSparkContext context = new JavaSparkContext(config);

        JavaRDD<String> flightsFile = context.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportsFile = context.textFile("L_AIRPORT_ID.csv");

        JavaPairRDD<String, String> pairs = airportsFile.
                filter(s -> !s.contains("Code")).
                mapToPair(
                        s -> {
                            int firstComma = s.charAt(',');
                            return new Tuple2<>(s.substring(0, firstComma), s.substring(firstComma + 1, s.length()));
                        }
                );

        JavaPairRDD<Tuple2<Integer, Integer>, SerializedData> flightData = flightsFile.
                filter(s -> s.contains("YEAR")).
                mapToPair(
                        s -> {
                            String[] columns = s.split(",");
                            int originAirportID = Integer.parseInt(columns[11]);
                            int destAirportID = Integer.parseInt(columns[14]);
                            float delay = Float.parseFloat(columns[18]);
                            float isCancelled = Float.parseFloat(columns[19]);
                            return new Tuple2<>(
                                    new Tuple2<>(originAirportID, destAirportID),
                                    new SerializedData(/*originAirportID, destAirportID,*/
                                            delay, isCancelled, delay, (0.00==isCancelled)?0:1,1)
                            );
                        }
                );
        JavaPairRDD<Tuple2<Integer,Integer>, SerializedData> result= flightData.reduceByKey(
                (a,b) -> new SerializedData(Math.max(a.getMaxDelay(),b.getMaxDelay()),
                        a.getAmountOfDelayedAndCanceled()+b.getAmountOfDelayedAndCanceled(),
                        a.getAmountOfAll()+b.getAmountOfAll())
        );

        Map<String, String> airportsMap = pairs.collectAsMap();

        final Broadcast<Map<String, String>> airportsBroadcasted = context.broadcast(airportsMap);

        JavaRDD<String> res = result.map(
                s -> "From: "+ airportsBroadcasted.value().get(s._1())+" -> "+ airportsBroadcasted.value().get(s._2)+
                        "\t"+s._2()
        );
        res.saveAsTextFile("hdfs://localhost:9000/user/madina/output");
    }
}
