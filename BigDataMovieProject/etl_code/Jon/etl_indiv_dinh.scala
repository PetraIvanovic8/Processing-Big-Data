// scala individual ETL -> basically data cleaning and formatting

// spark-shell --deploy-mode client -i  etl_indiv_dinh.scala

// imports
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature

val df_api = spark.read.options(Map("inferSchema"->"true","header"->"true")).csv("api_pull_replace_links.csv")
df_api.show(10, false) // show top 10 columns to verify
df_api.printSchema()

val df_ratings  = spark.read.options(Map("inferSchema"->"true","header"->"true")).csv("ratings.csv")
df_ratings.show(10, false) // show top 10 columns to verify
df_ratings.printSchema()

val df_movies = spark.read.options(Map("inferSchema"->"true","header"->"true")).csv("movies.csv")
df_movies.show(10, false) // show top 10 columns to verify
df_movies.printSchema()

val df_tags =spark.read.options(Map("inferSchema"->"true","header"->"true")).csv("tags.csv")
df_tags.show(10, false) // show top 10 columns to verify
df_tags.printSchema()


// aggregate ratingsCount and ratingsAvg per movie 
val res1: DataFrame = df_ratings.groupBy("movieId").agg(count("rating").as("smallLensRatingCount"), avg("rating").as("smallLensRatingAvg"))
res1.show(10,false)

// join ratings aggregate with df_api
val out: DataFrame = df_api.join(res1, Seq("movieId"), "left")
out.show(10, false)

// join df_api with df_movies
val out2: DataFrame = out.join(df_movies, Seq("movieId"), "left")
out2.show(10, false)

// join df_api with df_tag on movieId with the tagCounts per movie
val out3: DataFrame = out2.join(df_tags.groupBy("movieId").agg(count("tag").as("smallLensTagCount")), Seq("movieId"), "left")
out3.show(10, false)

val df = out3

/*
Schema
 |-- movieId: integer (nullable = true)
 |-- imdbId: string (nullable = true)
 |-- tmdbId: double (nullable = true)
 	[x] cast to integer (not really needed, but for cleanliness)
 |-- mediaType: string (nullable = true)
 	[x] lower
 |-- imdb_ratingCount: integer (nullable = true)
 |-- imdb_bestRating: integer (nullable = true)
 |-- imdb_worstRating: integer (nullable = true)
 |-- imdb_averageRating: double (nullable = true)
 	[x] limit decimal
 |-- contentRating: string (nullable = true)
 	[x] lower
 |-- releaseDate: string (nullable = true)
 	[x] split into year and month columns
 |-- smallLensRatingCount: integer (nullable = true)
 |-- smallLensRatingAvg: double (nullable = true)
 	[x] limit decimal
 |-- title: string (nullable = true)
 	[x] lower
 	[x] replace commas with spaces or dash
 |-- genres: string (nullable = true)
 	[x] lower
 |-- smallLensTagCount: integer (nullable = true)

*/


// lower all the non-ordinal columns
var tmp = df.withColumn("title", lower(col("title")))
tmp = tmp.withColumn("genres", lower(col("genres")))
tmp = tmp.withColumn("contentRating", lower(col("contentRating")))
tmp = tmp.withColumn("mediaType", lower(col("mediaType")))

// limit decimal places
tmp = tmp.withColumn("imdb_averageRating", round(col("imdb_averageRating"), 2))
tmp = tmp.withColumn("smallLensRatingAvg", round(col("smallLensRatingAvg"), 2))

// replace commas in the "title" column 
tmp = tmp.withColumn("title", regexp_replace($"title", ","," "))

// split the releaseDate column into year and month columns
tmp = tmp.withColumn("releaseYear", split(col("releaseDate"), "-")(0))
tmp = tmp.withColumn("releaseMonth", split(col("releaseDate"), "-")(1))
tmp = tmp.drop("releaseDate")

// re-enforce appropriate dtypes
tmp = tmp.withColumn("tmdbId", col("tmdbId").cast("int"))

// save to csv file in hdfs
tmp.write.option("header",true).csv("output_etl_indiv_dinh")