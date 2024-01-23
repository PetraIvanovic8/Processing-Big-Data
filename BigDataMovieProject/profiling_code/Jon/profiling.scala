// scala profiling -> helps with ETL is basic pipelining

// spark-shell --deploy-mode client -i  profiling.scala

// imports
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// read in output from data ingestion
val df = spark.read.options(Map("inferSchema"->"true","header"->"true")).csv("output_ingest.csv")
df.show(10, false) // show top 10 columns to verify

// check the schema for enforcement
df.printSchema()

println("Schema below;")
/*
root
 |-- movieId: integer (nullable = true)
 |-- imdbId: string (nullable = true)
 |-- tmdbId: double (nullable = true)
 |-- mediaType: string (nullable = true)
 |-- imdb_ratingCount: integer (nullable = true) 
 |-- imdb_bestRating: integer (nullable = true) 
 |-- imdb_worstRating: integer (nullable = true)
 |-- imdb_averageRating: double (nullable = true) 
 |-- contentRating: string (nullable = true)
 |-- releaseDate: string (nullable = true) -> categorical label encoding in ETL
 |-- smallLensRatingCount: integer (nullable = true)
 |-- smallLensRatingAvg: double (nullable = true)
 |-- title: string (nullable = true)
 |-- genres: string (nullable = true) -> categorical label encoding in ETL
 |-- smallLensTagCount: integer (nullable = true)
 */

// view counts of mediaType
val mediaCounts = df.groupBy("mediaType").count()
mediaCounts.show(10, false)

// view counts of contentRating
val contentCounts = df.groupBy("contentRating").count()
contentCounts.show(10, false)

// imdb_averageRating -> mean, median IMDb, STD 
// mode invalid from long decimal places -> trim in ETL
val avgAverageRatings = df.select(mean("imdb_averageRating")).show(false)
val medianAverageRatings = df.stat.approxQuantile("imdb_averageRating", Array(0.5), 0.01)(0)
val stdAverageRating = df.select(stddev("imdb_averageRating")).show(false)

// releaseDate -> counts
// uninformative and too specific -> trim down to year and month in ETL
val dateCounts = df.groupBy("releaseDate").count().show(false)

// smallLensRatingCount mean and median
val avgSmallLensRatingCount = df.select(mean("smallLensRatingCount")).show(false)
val medianSmallLensRatingCount = df.stat.approxQuantile("smallLensRatingCount", Array(0.5), 0.01)(0)

// smallLensAverageRating -> mean, median, STD
val avgSmallLensRatingAvg = df.select(mean("smallLensRatingAvg")).show(false)
val medianSmallLensRatingAvg = df.stat.approxQuantile("smallLensRatingAvg", Array(0.5), 0.01)(0)
val stdSmallLensRatingAvg = df.select(stddev("smallLensRatingAvg")).show(false)

// genres -> counts
val genresCount = df.groupBy("genres").count().show(false)

// smallLensTagCount -> mean, median, mode, STD
val avgSmallLensTagCount = df.select(mean("smallLensTagCount")).show(false)
val medianSmallLensTagCount = df.stat.approxQuantile("smallLensTagCount", Array(0.5), 0.01)(0)
val stdSmallLensTagCount = df.select(stddev("smallLensTagCount")).show(false)

/*
ETL Tasks:
lower mediatype
trim imdb average rating decimal 
lower content rating
split release date into month and day format
trim small lens rating avg
lower title
lower genres
categorical label encoding of release date year and month
categorical label encoding of genres
normalize integer and double columns
*/