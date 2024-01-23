// analytics

// spark-shell --deploy-mode client -i  ana_code_dinh.scala

// imports
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

// settings
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

// read in data
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("output_etl_comb_dinh.csv")

// shorten the column names for readability
var tmp = df
tmp = tmp.withColumn("title_lc", col("title_lowercase"))
tmp = tmp.withColumn("contentRate", col("contentRating"))
tmp = tmp.withColumn("rlYr", col("releaseYear"))
tmp = tmp.withColumn("type", col("mediaType"))
tmp = tmp.withColumn("rlMnth", col("releaseMonth"))
tmp = tmp.withColumn("rnTimeMin", col("runtimeMinutes"))
tmp = tmp.withColumn("gross", col("lifetime_gross"))
tmp = tmp.withColumn("imdbRtCnt", col("imdb_ratingCount"))
tmp = tmp.withColumn("imdbRtBest", col("imdb_bestRating"))
tmp = tmp.withColumn("imdbRtWorst", col("imdb_worstRating"))
tmp = tmp.withColumn("imdbRtAvg", col("imdb_averageRating"))
tmp = tmp.withColumn("slRateCnt", col("smallLensRatingCount"))
tmp = tmp.withColumn("slRateAvg", col("smallLensRatingAvg"))
tmp = tmp.drop("title_lowercase","releaseYear","releaseMonth", "runtimeMinutes", "lifetime_gross",
"imdb_ratingCount","imdb_bestRating","imdb_worstRating","imdb_averageRating","smallLensRatingCount",
"smallLensRatingAvg", "contentRating","mediaType", "primaryTitle")



// Encoding
// NOTE: Label Encoding -> for natural sequences with meaning (like month)
// NOTE: One Hot Encoding -> un-meaningful categories if ordered/ranked (title)
// Note: Just label encoding everything first, dealing with vector per col per record is a bit much rn
var tmp2 = tmp

// convert year(int) and month(int) to String for encoding
tmp2 = tmp2.withColumn("rlYr", col("rlYr").cast("string"))
tmp2 = tmp2.withColumn("rlMnth", col("rlMnth").cast("string"))

// columns to encode -> column names as output
val colToLabelEncode = Array("rlYr","rlMnth","contentRate", "studio", "type","category","directors","genres","primaryName")
val colEncoded = Array("rlYr_LE","primaryName_LE","rlMnth_LE","contentRate_LE", "studio_LE", "type_LE","category_LE","directors_LE","genres_LE")

// create indexer, transform
val indexer = new StringIndexer().setHandleInvalid("keep").setInputCols(colToLabelEncode).setOutputCols(colEncoded)
tmp2 = indexer.fit(tmp2).transform(tmp2)

// rename the new rows to the old name (just now they are encoded)
tmp2 = tmp2.drop("rlYr","rlMnth","contentRate", "studio", "type","category","directors","genres","primaryName")
tmp2 = tmp2.withColumn("rlYr", col("rlYr_LE"))
tmp2 = tmp2.withColumn("rlMnth", col("rlYr_LE"))
tmp2 = tmp2.withColumn("contentRate", col("contentRate_LE"))
tmp2 = tmp2.withColumn("studio", col("studio_LE"))
tmp2 = tmp2.withColumn("type", col("type_LE"))
tmp2 = tmp2.withColumn("category", col("category_LE"))
tmp2 = tmp2.withColumn("directors", col("directors_LE"))
tmp2 = tmp2.withColumn("genres", col("genres_LE"))
tmp2 = tmp2.withColumn("primaryName", col("primaryName_LE"))

// drop the old columns
tmp2 = tmp2.drop("primaryName_LE","rlYr_LE","rlMnth_LE","contentRate_LE", "studio_LE", "type_LE","category_LE","directors_LE","genres_LE")

// re-order the columns for sense
val reorderedCols = Array("title","title_lc","studio","directors","contentRate", "rnTimeMin", "genres",
"rlYr", "rlMnth", "type","primaryName", "category", "numVotes","imdbRtCnt","imdbRtBest","imdbRtWorst",
"imdbRtAvg","slRateCnt","slRateAvg","gross")
tmp2 = tmp2.select(reorderedCols.head, reorderedCols.tail: _*)
tmp2 = tmp2.na.fill(0)

// cast to all doubles
tmp2 = tmp2.withColumn("rnTimeMin", col("rnTimeMin").cast("double"))
tmp2 = tmp2.withColumn("gross", col("gross").cast("double"))
tmp2 = tmp2.withColumn("numVotes", col("numVotes").cast("double"))
tmp2 = tmp2.withColumn("imdbRtCnt", col("imdbRtCnt").cast("double"))
tmp2 = tmp2.withColumn("imdbRtBest", col("imdbRtBest").cast("double"))
tmp2 = tmp2.withColumn("imdbRtWorst", col("imdbRtWorst").cast("double"))
tmp2 = tmp2.withColumn("slRateCnt", col("slRateCnt").cast("double"))

// convert chosen features
var tmp3 = tmp2.select("studio","directors","contentRate", "rnTimeMin", "genres", 
"rlYr", "rlMnth", "type","primaryName", "category", "numVotes","imdbRtCnt","imdbRtBest","imdbRtWorst",
"imdbRtAvg","slRateCnt","slRateAvg","gross")

// convert to RDD
val rows = new VectorAssembler().setInputCols(tmp3.columns).setOutputCol("vector").transform(tmp3).select("vector").rdd
val rdd = rows.map(_.getAs[org.apache.spark.ml.linalg.Vector](0)).map(org.apache.spark.mllib.linalg.Vectors.fromML)

// get the basic descriptive statistics/analytics
// summary.[max, min, variance, mean, numNonzeros]
val summary: MultivariateStatisticalSummary = Statistics.colStats(rdd)

// [1.0,714.0,329.0,366.0,2501.0,93.0,93.0,4.0,12.0,2758.0,2814806.0,2819259.0,10.0,1.0,9.3,329.0,5.0,7.60507625E8]
summary.max 

// [0.0,0.0,0.0,26.0,0.0,0.0,0.0,0.0,0.0,0.0,21.0,0.0,0.0,0.0,0.0,0.0,0.0,252.0]
summary.min

// [0.1559762202753442,72.27816020025018,24.693992490613237,107.38250938673342,614.2953692115144,
// 28.908166458072625,28.908166458072625,0.30506883604505597,6.333072590738417,769.9974968710873,
// 108181.5120463078,93492.6749061328,8.440237797246558,0.8440237797246558,5.5392052565707255,13.4701188986233,
// 3.1826861702127673,3.8618891159261525E7]
summary.mean

// [0.13166823789477106,18291.559291372818,2199.7458064953303,359.1293638788041,492438.48636253015,
// 934.3578616352512,934.3578616352512,0.2902700312489651,16.289452113021134,603601.0999780884,3.8911327035719505E10,
// 3.617054620960797E10,13.166823789477107,0.13166823789477106,6.411446590940938,653.29154947733,0.6377810324608746,
/// 3.8321317971397395E15]
summary.variance

// [997.0,5625.0,5804.0,6392.0,6356.0,6184.0,6184.0,1725.0,5872.0,6350.0,6392.0,5395.0,5395.0,5395.0,5395.0,
// 6383.0,6383.0,6392.0]
summary.numNonzeros

// get the correlation pairwise using Pearson between all features and variable of interest
val corrMatrix: Matrix = Statistics.corr(rdd, "pearson")
corrMatrix

// write out to csv for visualization, because Spark documention is ...
tmp.coalesce(1).write.option("header",true).csv("pre_process")
tmp2.coalesce(1).write.option("header",true).csv("process")



// Extra code
// Normalizing appropriate columns if needed manually
/*
val colToScale = Array("lifetime_gross","imdb_ratingCount","imdb_bestRating","imdb_worstRating",
"imdb_averageRating","smallLensRatingCount","smallLensRatingAvg","numVotes","runtimeMinutes")
var tmp2 = tmp

for (i <- colToScale) {
	tmp2 = tmp2.withColumn(i, col(i).cast("double"))
	val (mean_i, std_i) = tmp2.select(mean(i), stddev(i)).as[(Double, Double)].first()
	val (min_i, max_i) = tmp.select(min(i), max(i)).as[(Double, Double)].first()
	// tmp2 = tmp2.withColumn(i, (col(i) - mean_i)/std_i)
	tmp2 = tmp2.withColumn(i, (col(i) - min_i)/(max_i-min_i))
}
tmp2.show(5, true)
*/
