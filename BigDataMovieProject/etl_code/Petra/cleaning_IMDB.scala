//spark-shell --deploy-mode client

//load and explore datasets - does not need to be ran as we ran it in data ingest part 
/*

val namesDF = spark.read.option("delimiter", "\t").option("header", "true").csv("name.basics.tsv")
//namesDF.show()
namesDF.printSchema()

val titlesDF = spark.read.option("delimiter", "\t").option("header", "true").csv("title.basics.tsv")
//titlesDF.show()
titlesDF.printSchema()

val crewDF = spark.read.option("delimiter", "\t").option("header", "true").csv("title.crew.tsv")
//crewDF.show()
crewDF.printSchema()


val principalsDF = spark.read.option("delimiter", "\t").option("header", "true").csv("title.principals.tsv")
//principalsDF.show()
principalsDF.printSchema()

val ratingsDF = spark.read.option("delimiter", "\t").option("header", "true").csv("title.ratings.tsv")
//ratingsDF.show()
ratingsDF.printSchema()

*/

// Let's explore our separate datsets:
//count number of records in datasets and unique keys in the datasets 
val namesCount = namesDF.count() //12986658
val namesKeys = namesDF.select("nconst").distinct().count()//12986658

val titlesCount = titlesDF.count() //10290694  
val titlesKeys = titlesDF.select("tconst").distinct().count() //10290694

val crewCount = crewDF.count() //10290694
val crewKeys = crewDF.select("tconst").distinct().count() //10290694

val principalsCount = principalsDF.count() //58943052
val principalsKeys = principalsDF.select("tconst").distinct().count() //9317696
		// for each title there is multiple important people 
// lets only kep the top person 
val principalsDFNew = principalsDF.where($"ordering" === 1)
principalsDFNew.count() //9317696

val ratingsCount = ratingsDF.count() //1366349
val ratingsKeys = ratingsDF.select("tconst").distinct().count() //1366349


// Merging 5 datasets/tables into 1 main one

// Join titleDF with ratings on 'tconst' key
val titlesRatingsMerged = titlesDF.join(ratingsDF, Seq("tconst"), "left")

// Join with crewDF 
val titlesRatingsCrewMerged = titlesRatingsMerged.join(crewDF, Seq("tconst"), "left")

// Join with principalsDF 
val titlesRatingsCrewPrincipalsMerged = titlesRatingsCrewMerged.join(principalsDFNew, Seq("tconst"), "left")

// Join with namesDF 
val imdbTable = titlesRatingsCrewPrincipalsMerged.join(namesDF, Seq("nconst"), "left")

// Show the merged DataFrame
//imdbTable.show()

// drop rows with no titles or ratings 
val imdbTableNew= imdbTable.na.drop("any", Seq("primaryTitle", "averageRating"))

// Show the result 
//imdbTableNew.show()
//imdbTableNew.count()

// Remove unnecessary columns
val columnsToRemove = Seq("ordering", "characters", "birthYear", "deathYear", 
	"primaryProfession", "knownForTitles", "job", "endYear")
val imdbTableDF = imdbTableNew.drop(columnsToRemove: _*)

//imdbTableDF.show()

// Drop rows that have NA for Primary Title 
val columnToCheck = "primaryTitle"
val imdbTableDF_new = imdbTableDF.na.drop(Seq(columnToCheck))


// Filter data for it to only include 'movies'  
val distinctTitleTypes = imdbTableDF_new.select("titleType").distinct()
//distinctTitleTypes.show()

val imdbTableDF = imdbTableDF_new.filter(col("titleType").isin("movie", "tvMovie"))
//imdbTableDF.show()

// Let's see how many rows vs how many unique movies we have
val numRows = imdbTableDF.count() //351005 
val uniqueMovies = imdbTableDF.select("tconst").distinct().count() //351005 

// We can see that there are A LOT of duplicate movies, let's drop duplicates
val imdbTable= imdbTableDF.dropDuplicates("tconst")
imdbTable.count() //351005   

// Let's turn startYear & runtimeMinutes & averageRating & isAdult into numerical rather than string columns
val imdbTableNew = imdbTable.withColumn("startYearInt", col("startYear").cast("int"))
val imdbTable = imdbTableNew.drop("startYear")

val imdbTableNew = imdbTable.withColumn("runtimeMinutesInt", col("runtimeMinutes").cast("int"))
var imdbTable = imdbTableNew.drop("runtimeMinutes")

val imdbTableNew = imdbTable.withColumn("averageRatingInt", col("averageRating").cast("int"))
var imdbTable = imdbTableNew.drop("averageRating")

val imdbTableNew = imdbTable.withColumn("isAdultInt", col("isAdult").cast("int"))
var imdbTable = imdbTableNew.drop("isAdult")

imdbTable = imdbTable.withColumnRenamed("startYearInt", "startYear")
imdbTable = imdbTable.withColumnRenamed("runtimeMinutesInt", "runtimeMinutes")
imdbTable = imdbTable.withColumnRenamed("averageRatingInt", "averageRating")
imdbTable = imdbTable.withColumnRenamed("isAdultInt", "isAdult")

imdbTable.printSchema()

// Let's save our data into hdfs 
imdbTable.write
  .format("csv") 
  .option("header", "true") 
  .mode("overwrite") 
  .save("IMDBdata_joined_raw")

