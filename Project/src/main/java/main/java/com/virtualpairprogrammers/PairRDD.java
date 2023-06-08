package main.java.com.virtualpairprogrammers;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import scala.Tuple2;

import static org.apache.spark.sql.functions.*;

public class PairRDD {
	public static void main(String[] args) {
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 Semptember 0405");
		inputData.add("ERROR: Tuesday 4 Semptember 0408");
		inputData.add("FATAL: Wednesday 5 Semptember 1632");
		inputData.add("ERROR: Friday 7 Semptember 1854");
		inputData.add("WARN: Saturday 8 Semptember 1942");

		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> originalLogMessages = sc.parallelize(inputData);

		JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair( rawValue -> { 
			String[] columns = rawValue.split(":");
			String level = columns[0];

			return new Tuple2<>(level, 1L);
		} );
		
		
		JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
		sumsRdd.collect().forEach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

//		sc.parallelize(inputData)
//      	.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0] , 1L  ))
//			.reduceByKey((value1, value2) -> value1 + value2)
//			.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		
		
		sc.close();
	}
}
