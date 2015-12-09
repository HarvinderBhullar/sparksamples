package com.hs.regression;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.Normalizer;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;

import scala.Tuple2;

public class LinearRegressionSparkMain {

	/**
	 * The data is organized in the following way, current is label 
	 * and all the others except type are features
	 * 
	 * col[1] = lable
	 * col[2..4] = features
	 * 
	 * Ignore the last column
	 *   
	 *  current,temperature,pressure,humidity,type
	 * 
	 */
	static class ParsePoint implements Function<String, LabeledPoint> {
		private static final Pattern COMMA = Pattern.compile(",");

		@Override
		public LabeledPoint call(String line) {
			String[] parts = COMMA.split(line);
			double y = Double.parseDouble(parts[0]);
			double[] x = new double[parts.length - 2];
			for (int i = 1; i < parts.length - 1; i++) {
				x[i - 1] = Double.parseDouble(parts[i]);
			}
			return new LabeledPoint(y, Vectors.dense(x));
		}
	}

	public static void main(String args[]) {

		SparkConf sparkConf = new SparkConf().setAppName("LinearRegression");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile("lathe.csv");

		// get the header
		String header = lines.first();
		// remove the header line
		lines = lines.filter(p -> !p.equalsIgnoreCase(header));
		JavaRDD<LabeledPoint> points = lines.map(new ParsePoint()).cache();

		JavaRDD<LabeledPoint> pointNormalized = normalizeData(points);

		// Building the model
		int numIterations = 1000;
		final LinearRegressionModel model = LinearRegressionWithSGD.train(
				JavaRDD.toRDD(pointNormalized), numIterations);
		
		JavaRDD<Tuple2<Double, Double>> valuesAndPreds = pointNormalized.map( point -> {
			double prediction = model.predict(point.features());
			return new Tuple2<Double, Double>(prediction, point
					.label());
		});
		//find the RMSE
		double RMSE = Math.pow(valuesAndPreds.mapToDouble(pair -> Math.pow(pair._1() - pair._2(), 2.0)).mean(), .5);
		System.out.println("Training Root Mean Squared Error = " + RMSE);
		// Save the model
		model.save(sc.sc(), "modelPath");
		sc.stop();
	}

	private static JavaRDD<LabeledPoint> normalizeData(
			JavaRDD<LabeledPoint> points) {
		Normalizer normalizer = new Normalizer();
		return points.map(x -> (new LabeledPoint(x.label(), normalizer
				.transform(x.features()))));

	}

}