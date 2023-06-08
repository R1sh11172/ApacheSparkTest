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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

public class Main {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		
		//System.setProperty("hadoop.home.dir", "/Users/rishianiga/Downloads/Practicals/winutils-extra/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .getOrCreate();

		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		dataset.show();
		
		//long numberOfRows = dataset.count();
		//System.out.println(numberOfRows);
		
		Row firstRow = dataset.first();
		String subject = firstRow.getAs("subject").toString();
		int year = Integer.parseInt(firstRow.getAs("year"));
		
		Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year >= 2007 ");
//		Dataset<Row> modernArtResults = dataset2.filter("select * from students where subject = 'Modern Art' AND year >= 2007 ");
		
		
//		Dataset<Row> modernArtResults = dataset.filter( row -> row.getAs("subject").equals("Modern Art")
//									&& Integer.parseInt(row.getAs("year")) >= 2007);			
//		Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
//									.and(col("year").geq(2007)));
		modernArtResults.show();
//
//		dataset = dataset.select(col("level"),
//                date_format(col("datetime"), "MMMM").alias("month"), 
//                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
//		
//		dataset.groupBy(col("level"), col("month"), col("monthnum")).count();
//		dataset = dataset.orderBy(col("monthnum"));
//		dataset = dataset.drop(col("monthnum"));
//
//		dataset.show(100);
//		
		Dataset<Row> dataset2 = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		dataset2.createOrReplaceTempView("logging_table");
		Dataset<Row> results = spark.sql
		  ("select level, date_format(datetime,'MMMM') as month, count(1) as total " + 
		   "from logging_table group by level, month order by cast(first(date_format(datetime,'M')) as int), level");	
		
		
//		dataset.createOrReplaceTempView("my_students_table"); //alternate name in memory
//		Dataset<Row> results = spark.sql("select score, year from my_students_table where subject='French'");
//		results.show();

//		List<Row> inMemory = new ArrayList<Row>();
//		inMemory.add(RowFactory.create("WARN", "16 December 18 04:19:32"));
//		StructField[] fields = new StructField[] {
//				new StructField("level", DataTypes.StringType, false, Metadata.empty()),
//				new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
//		};
//		StructType schema = new StructType(fields);
//		spark.createDataFrame(inMemory, schema);
		
		
		
		
		spark.close();
	}

}
