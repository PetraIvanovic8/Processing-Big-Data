import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

//Parsing csv
val imdbTable = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/pi2018_nyu_edu/moviesFinalData_Cleaned/")

//Finding most mentioned actors 
// Checking that they are actors
val actorTable = imdbTable.filter("category == 'actor'")

//Group by name and count 
val actorCount = actorTable.groupBy("primaryName").count()

//Order by the count 
val sortedActorCounts = actorCount.orderBy(desc("count"))

// Take the top 10
val top10Actors = sortedActorCounts.limit(10)

// Show the result
top10Actors.show()


//finding actors with the highest average lifetime gross 
val actorData = imdbTable.filter(col("category") === "actor")

//Grouping by name
val groupActors = actorData.groupBy("primaryName")

//Calculate the average lifetime gross per actor
val actorGrossResult = groupActors.agg(avg("lifetime_gross").as("avgLifetimeGross"))

// Rank actors based on average lifetime gross
val grossWindowSpec = Window.orderBy(desc("avgLifetimeGross"))
val rankedActors = actorGrossResult.withColumn("rank", dense_rank().over(grossWindowSpec))

// showing the top 10 actors with the highest average lifetime gross
val top10ActorsGross = rankedActors.filter("rank <= 10")
top10ActorsGross.show()


//calculating average IMDb rating for each actor's movies
val actorsRatingResult = groupActors.agg(avg("imdb_averageRating").alias("averageRating"))

//order the results
val ratingWindowSpec = Window.orderBy(col("averageRating").desc)
val sortedActorsRating = actorsRatingResult.withColumn("rank", rank().over(ratingWindowSpec))

//showing top 10
val top10ActorsRating = sortedActorsRating.filter(col("rank") <= 10).select("primaryName", "averageRating")
top10ActorsRating.show()



// Finding most mentioned actresses 
// Checking that they are actresses
val actressTable = imdbTable.filter("category == 'actress'")

// Group by name and count occurrences
val actressCount = actressTable.groupBy("primaryName").count()

// Order by the count 
val sortedActressCounts = actressCount.orderBy(desc("count"))

// Take the top 10
val top10Actresses = sortedActressCounts.limit(10)

// Show the result
top10Actresses.show()

// Finding actresses with the highest average lifetime gross 
//gathering actresses
val actressData = imdbTable.filter(col("category") === "actress")

// Grouping by name
val groupActresses = actressData.groupBy("primaryName")

//Calculate average lifetime gross
val actressesGrossResult = groupActresses.agg(avg("lifetime_Gross").alias("averageLifetimeGross"))

// Order the results by average lifetime gross
val grossWindowSpecActresses = Window.orderBy(col("averageLifetimeGross").desc)
val sortedActressesGross = actressesGrossResult.withColumn("rank", rank().over(grossWindowSpecActresses))

// Show the top 10 
val top10ActressesGross = sortedActressesGross.filter(col("rank") <= 10).select("primaryName", "averageLifetimeGross")
top10ActressesGross.show()


// Calculate average IMDb rating for each actress' movies
val actressesRatingResult = groupActresses.agg(avg("imdb_averageRating").alias("averageRating"))

//order the results
val ratingWindowSpecActresses = Window.orderBy(col("averageRating").desc)
val sortedActressesRating = actressesRatingResult.withColumn("rank", rank().over(ratingWindowSpecActresses))

//show top 10 
val top10ActressesRating = sortedActressesRating.filter(col("rank") <= 10).select("primaryName", "averageRating")
top10ActressesRating.show()
