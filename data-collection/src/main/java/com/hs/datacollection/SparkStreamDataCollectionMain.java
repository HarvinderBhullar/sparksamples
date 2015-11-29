package com.hs.datacollection;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.google.common.collect.Lists;

public class SparkStreamDataCollectionMain {
	private static final int SLIDING_INTERVAL = 10000; // in ms

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("DataCollection").set("spark.cassandra.connection.host",
				"127.0.0.1");
		;
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(SLIDING_INTERVAL));
		JavaReceiverInputDStream<String> rawData = ssc.receiverStream(new CustomHttpReceiver());

		
		JavaDStream<String> d = rawData.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String x) {
				return Lists.newArrayList(x);
			}
		});
		persistRawData(d);
		ssc.start();
		ssc.awaitTermination();
	}

	private static void persistRawData(JavaDStream<String> data) {
		data.foreachRDD(d -> {
			if (d.count() == 0) {
				return null;
			}

			JavaRDD<String> rowRDD = d.map(p -> {
				return p;
			});
			SQLContext sqlContext = SQLContext.getOrCreate(d.context());
			DataFrame jsonData = sqlContext.read().json(rowRDD);
			List<String> colNameList = Arrays.asList(jsonData.columns());
			if (colNameList.contains("current")) {
				HashMap<String, String> map = new HashMap<>();
				map.put("table", "machine_data");
				map.put("keyspace", "machinedata");
				DataFrame mData = jsonData.withColumnRenamed("timestamp", "reading_time");
				//mData.printSchema();
				mData.write().mode(SaveMode.Append).format(
				 "org.apache.spark.sql.cassandra").options(map).save();
			} 
			return null;
		});

	}

}
