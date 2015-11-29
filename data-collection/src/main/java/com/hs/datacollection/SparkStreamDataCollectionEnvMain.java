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

public class SparkStreamDataCollectionEnvMain {
	private static final int SLIDING_INTERVAL = 10000; // in ms

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("DataCollectionEnv").set("spark.cassandra.connection.host",
				"127.0.0.1");
		;
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(SLIDING_INTERVAL));
		JavaReceiverInputDStream<String> webServiceResponse = ssc.receiverStream(new CustomHttpReceiverEnv());

		JavaDStream<String> data = webServiceResponse.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String x) {
				return Lists.newArrayList(x);
			}
		});
	
		persistEnvData(data);
		ssc.start();
		ssc.awaitTermination();
	}

	private static void persistEnvData(JavaDStream<String> data) {
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
			if (colNameList.contains("pressure")) {
				HashMap<String, String> map = new HashMap<>();
				map.put("table", "env_data");
				map.put("keyspace", "machinedata");
				//jsonData.printSchema();

				List<EnvironmentData> envDataList = jsonData.javaRDD().map(new Function<Row, EnvironmentData>() {
					public EnvironmentData call(Row row) {
						EnvironmentData env = new EnvironmentData();
						env.setReadtime((String) row.getList(0).get(0));
						env.setHumidity(Double.parseDouble((String) row.getList(0).get(1)));
						env.setPressure(Double.parseDouble((String) row.getList(1).get(1)));
						env.setTemperature(Double.parseDouble((String) row.getList(2).get(1)));
						return env;
					}
				}).collect();
				saveToCassandra(d.context(), envDataList);
				}
			return null;
		});

	}

	private static void saveToCassandra(SparkContext context, List<EnvironmentData> envDataList) {
		final CassandraConnector connector = CassandraConnector.apply(context.conf());
		envDataList.forEach(p -> {
			Statement statement = QueryBuilder.insertInto("machinedata", "env_data").value("readtime", p.getReadtime())
					.value("pressure", p.getPressure()).value("temperature", p.getTemperature())
					.value("humidity", p.getHumidity());
			try (Session session = connector.openSession()) {
				session.executeAsync(statement);
			}

		});
	}

}
