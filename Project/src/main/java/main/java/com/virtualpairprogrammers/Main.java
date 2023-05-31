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

import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		// SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		// JavaSparkContext sc = new JavaSparkContext(conf);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

		//Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		// Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007 ");

//		Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
//									.and(col("year").geq(2007)));
//
//		modernArtResults.show();

		dataset = dataset.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"), 
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
		
		dataset.groupBy(col("level"), col("month"), col("monthnum")).count();
		dataset = dataset.orderBy(col("monthnum"));
		dataset = dataset.drop(col("monthnum"));

		dataset.show(100);
		
		spark.close();

		
		
		
		
		
//		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
//		
//		JavaRDD<String> lettersOnlyRdd = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase() );
//		
		
//		JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter( sentence -> sentence.trim().length() > 0 );
//		
//		JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
//		
//		JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
//		
//		JavaRDD<String> justInterestingWords = blankWordsRemoved.filter(word -> Util.isNotBoring(word));
//		
//		JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));
//		
//		JavaPairRDD<String, Long> totals = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
//		
//		JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1 ));
//		
//		JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
//		
//		List<Tuple2<Long,String>> results = sorted.take(10);
//		
//		results.forEach(System.out::println);		
//		
		
		
		
		
		
		
		
		//JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
		
		
//		JavaRDD<Integer> myRDD = sc.parallelize(inputData);
//		sc.parallelize(inputData)
//			.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
//			.filter(word -> word.length() > 1) 
//			.collect().forEach(System.out::println);
		
		//Integer result = myRDD.reduce((value1, value2) -> value1 + value2);
		
		//JavaRDD<IntegerWithSquareRoot> sqrtRdd = myRDD.map( value -> new IntegerWithSquareRoot(value) ); //because RDD are immutable
		//JavaRDD<Tuple2<Integer, Double>> sqrtRdd = myRDD.map( value -> new Tuple2<> (value, Math.sqrt(value)) ); //because RDD are immutable

		//Tuple2<Integer, Double> myValue = new Tuple2<> (9, 3.0);
//		sqrtRdd.collect().forEach( System.out::println );
		
//		System.out.println(result);
		
//		//how many elements in sqrtRdd
//		System.out.println(sqrtRdd.count());
//		//count with map and reduce
//		JavaRDD<Long> singleIntegerRdd = sqrtRdd.map( value -> 1L);
//		Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
//		System.out.println(count);
		
		//sc.close();
	}

}
