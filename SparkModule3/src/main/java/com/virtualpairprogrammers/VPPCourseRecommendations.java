package com.virtualpairprogrammers;

import static org.apache.spark.sql.functions.*; 
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionSummary;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class VPPCourseRecommendations {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder()
				.appName("VPP Free Trials")
				.master("local[*]").getOrCreate();
		
		
		Dataset<Row> csvData = spark.read()
				.option("header", true)
				.option("inferSchema", true)
				.csv("src/main/resources/VPPcourseViews.csv");
		
		csvData = csvData.withColumn("proportionWatched" , col("proportionWatched").multiply(100));
		
		//csvData.groupBy("userId").pivot("courseId").sum("proportionWatched").show();
		
		ALS als = new ALS()
				.setMaxIter(10)
				.setRegParam(0.1)
				.setUserCol("userId")
				.setItemCol("courseId")
				.setRatingCol("proportionWatched");
		
		ALSModel model = als.fit(csvData);
		
		Dataset<Row> userRecs = model.recommendForAllUsers(5);
		
		List<Row> userRecsList = userRecs.takeAsList(5);
		
		for (Row r : userRecsList) {
			int userId = r.getAs(0);
			String recs = r.getAs(1).toString();
			System.out.println("User " + userId + " we might want to recommend " + recs);
			System.out.println("This user has already watched: ");
			csvData.filter("userId = " + userId).show();
		}
	}

}
