package com.hs.movingaverage;

import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;

public class SparkStreamMain {

	private static DataFrame alarmData;
	private static final int SLIDING_INTERVAL = 1000; // in ms
	private static final int WINDOW_INTERVAL = 1; // in minute

	public static void main(String args[]) {
		SparkConf sparkConf = new SparkConf().setAppName("MovingAverage").set("spark.cassandra.connection.host", "127.0.0.1");;
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				new Duration(SLIDING_INTERVAL));
		JavaReceiverInputDStream<String> webServiceResponse = ssc
				.receiverStream(new CustomHttpReceiver());

		JavaDStream<String> data = webServiceResponse.flatMap(
				new FlatMapFunction<String, String>() {
					public Iterable<String> call(String x) {
						return Lists.newArrayList(x);
					}
				}).cache();

		checkAlarms(data);
		movingAverage(data);
		ssc.start();
		ssc.awaitTermination();
	}

	private static void checkAlarms(JavaDStream<String> data) {
		alarmData = null;
		// final DataFrame alarmData;
		data.foreachRDD(d -> {
			if (d.count() == 0) {
				return null;
			}

			JavaRDD<String> rowRDD = d.map(p -> {
				return p;
			});
			SQLContext sqlContext = SQLContext.getOrCreate(d.context());
			DataFrame jsonData = sqlContext.read().json(rowRDD);
			String[] cols = jsonData.columns();
			if (Arrays.asList(jsonData.columns()).contains("current")) {
				alarmData = jsonData.filter(jsonData.col("current").gt(
						jsonData.col("current_alert")));
			}
			return null;
		});

	}

	private static void movingAverage(JavaDStream<String> data) {
		JavaDStream<String> windowDStream = data.window(Durations.minutes(WINDOW_INTERVAL),
				new Duration(SLIDING_INTERVAL));
		// final DataFrame alarmData;
		windowDStream.foreachRDD(d -> {
			if (d.count() == 0) {
				return null;
			}

			JavaRDD<String> rowRDD = d.map(p -> {
				return p;
			});
			SQLContext sqlContext = SQLContext.getOrCreate(d.context());
			DataFrame jsonData = sqlContext.read().json(rowRDD);
			String[] cols = jsonData.columns();
			if (Arrays.asList(jsonData.columns()).contains("current") && alarmData != null ) {
				if(alarmData.count() > 0)  {
					DataFrame movingAverage = jsonData.groupBy("machine_id").avg("current").withColumnRenamed("machine_id", "machineId_1");
					DataFrame mergedData = alarmData.join(movingAverage,alarmData.col("machine_id").equalTo(movingAverage.col("machineId_1"))).drop("machineId_1")
							.withColumnRenamed("timestamp", "reading_time")
							.withColumnRenamed("avg(current)", "avg_current");
					mergedData.printSchema();
					HashMap<String, String> map = new HashMap<>();
					map.put("table", "moving_average");
					map.put("keyspace", "machinedata");
					mergedData.write().mode(SaveMode.Append).format("org.apache.spark.sql.cassandra")
					  .options(map)
					  .save();
					
				}
			}
			return null;
		});

	}
}
