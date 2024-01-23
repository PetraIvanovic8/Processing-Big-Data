
// THE FILE IS IF USING CLUSTERING FOR ANALYTICS

/* example KNN clusering from spark.apache.org

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

// Loads data.
val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

// Trains a k-means model.
val kmeans = new KMeans().setK(2).setSeed(1L)
val model = kmeans.fit(dataset)

// Make predictions
val predictions = model.transform(dataset)

// Evaluate clustering by computing Silhouette score
val evaluator = new ClusteringEvaluator()

val silhouette = evaluator.evaluate(predictions)
println(s"Silhouette with squared euclidean distance = $silhouette")

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)

*/

// spark-shell --deploy-mode client -i  ana_code_dinh.scala

// imports
import org.apache.spark.ml.feature.StandardScaler
//import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderModel}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler

/*
// normalize appropriate columns manually, because I'm not dealing with VectorAssembler today no sir. 
val colToScale = Array("lifetime_gross","imdb_ratingCount","imdb_bestRating","imdb_worstRating","imdb_averageRating","smallLensRatingCount","smallLensRatingAvg","numVotes","runtimeMinutes")
var tmp2 = tmp

for (i <- colToScale) {
	tmp2 = tmp2.withColumn(i, col(i).cast("double"))
	val (mean_i, std_i) = tmp2.select(mean(i), stddev(i)).as[(Double, Double)].first()
	val (min_i, max_i) = tmp.select(min(i), max(i)).as[(Double, Double)].first()
	// tmp2 = tmp2.withColumn(i, (col(i) - mean_i)/std_i)
	tmp2 = tmp2.withColumn(i, (col(i) - min_i)/(max_i-min_i))
}
tmp2.show(5, true)

// NOTE: Label Encoding -> for natural sequences with meaning (like month)
// NOTE: One Hot Encoding -> un-meaningful categories if ordered/ranked (title

// label encoding 
val colToLabelEncode = Array("releaseYear","releaseMonth","contentRating")
var tmp3 = tmp2
tmp3 = tmp3.withColumn("releaseYear", col("releaseYear").cast("string"))
tmp3 = tmp3.withColumn("releaseMonth", col("releaseMonth").cast("string"))

for (i <- colToLabelEncode) {
	val indexer = new StringIndexer().setInputCol(i).setOutputCol(i+"_idxd").setHandleInvalid("keep")
	tmp3 = indexer.fit(tmp3).transform(tmp3)
}

tmp3 = tmp3.drop("releaseYear","releaseMonth","contentRating")
tmp3.show(5, true)
*/




// read in output from data ingestion
val df = spark.read.options(Map("inferSchema"->"true","header"->"true")).csv("output_etl_comb_dinh.csv")
df.show(10, false) // show top 10 columns to verify

// one hot encoding -> for sequences that are not usually meaningful when ordered/ranked
val colToOneHot = Array("studio", "mediaType","category","directors","genres")
var tmp1 = df
for (i <- colToOneHot) {
	val indexer = new StringIndexer().setInputCol(i).setOutputCol(i+"_idxd").setHandleInvalid("keep")
	tmp1 = indexer.fit(tmp1).transform(tmp1)

	val encoder = new OneHotEncoder().setInputCol(i+"_idxd").setOutputCol(i+"_vec").setHandleInvalid("keep")
	tmp1 = encoder.fit(tmp1).transform(tmp1)
}

tmp1 = tmp1.drop("studio", "mediaType","category","directors","genres")
tmp1 = tmp1.drop("studio_idxd", "mediaType_idxd","category_idxd","directors_idxd","genres_idxd")
tmp1 = tmp1.withColumn("imdb_bestRating", col("imdb_bestRating").cast("double"))
tmp1 = tmp1.withColumn("imdb_worstRating", col("imdb_worstRating").cast("double"))


// trains a k means model
val featCols = Array("lifetime_gross","imdb_ratingCount","imdb_bestRating","imdb_worstRating","smallLensRatingCount",
"smallLensRatingAvg","numVotes","runtimeMinutes","releaseYear_idxd","releaseMonth_idxd","contentRating_idxd",
"studio_vec","mediaType_vec", "category_vec","directors_vec","genres_vec")
val assembler = new VectorAssembler().setInputCols(featCols).setOutputCol("features").setHandleInvalid("keep")
val featureVector = assembler.transform(tmp1)


// ISSUE: Spark TaskSetManager is losing tasks in later stages during fitting stage when computing;
// EuclideanDistanceMeasure.findClosest
// EuclideanDistanceMeasure$.fastSquaredDistance
// fastSquaredDistance

val kmeans = new KMeans().setK(10).setSeed(1L)
val model = kmeans.fit(featureVector)
val data = model.transform(vectoredInput)

