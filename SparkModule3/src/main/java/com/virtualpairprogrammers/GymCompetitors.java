package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GymCompetitors {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("Gym Competitors")
				.config("spark.sql.warehouse.dir", "file:///c:/tmp/")
				.master("local[*]").getOrCreate();
		
		Dataset<Row> csvData = spark.read().option("header", true).csv("src/main/resources/GymCompetition.csv");
		
		csvData.show();
	}

}
