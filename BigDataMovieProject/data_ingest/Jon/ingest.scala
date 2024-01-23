// scala data ingest and personal dataset join

// spark-shell --deploy-mode client -i  ingest.scala


// import
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// read in csv

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

// join df_api with df_tags on movieId with String tags as a set
// NOTE: CSV does not support sets -> if want to use specifics of tags data -> split + one hot encoding
// NOTE: most observations (movies) are not tagged (therefore NULL) -> categorical variables are not intuitively conducive
// NOTE: previous tagCounts are intuitively more informative and captures some relationship already.
// val out4: DataFrame = out3.join(df_tags.groupBy("movieId").agg(collect_set("tag").as("smallLensTagList")), Seq("movieId"), "left")
// out4.show(10, false)

// data is ingested -> save as output
out3.write.option("header",true).csv("output_ingest")
