package main.java.com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

public class Main {

	public static void main(String[] args) {
		List<Integer> inputData = new ArrayList<Integer>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> myRDD = sc.parallelize(inputData);
		
		Integer result = myRDD.reduce((value1, value2) -> value1 + value2);
		
		JavaRDD<Double> sqrtRdd = myRDD.map( value -> Math.sqrt(value) ); //because RDD are immutable
		
		sqrtRdd.collect().forEach( System.out::println );
		
		System.out.println(result);
		
		//how many elements in sqrtRdd
		System.out.println(sqrtRdd.count());
		//count with map and reduce
		JavaRDD<Long> singleIntegerRdd = sqrtRdd.map( value -> 1L);
		Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
		System.out.println(count);
		
		sc.close();
	}

}
