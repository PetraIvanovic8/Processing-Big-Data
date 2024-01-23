// Let's merge all 3 datasets

// Load IMDB data 
val imdbTable = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/pi2018_nyu_edu/IMDBdata_joined/")
// Let's remove genres to not have copies of the same column 
val imdbData = imdbTable.drop("genres")

// Load Jon's movie data
val data = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/jhd9252_nyu_edu/output_etl_indiv_dinh.csv")
data.show()

// Let's join imdbTable and data table into one
val joinedTable = data.join(imdbData, data("imdbId") === imdbData("tconst"), "inner")
// Let's rename the title column to not have duplicates
val joinedData = joinedTable.withColumnRenamed("title", "title_lowercase")

// Let's see the current joint data
joinedData.show()


// Let's load the box office data 
val boxoffice = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/na2794_nyu_edu/cleanboxoffice.csv/")
boxoffice.show()

// Let's finally join the last dataset
val movies = boxoffice.join(joinedData, boxoffice("title") === joinedData("primaryTitle"), "inner")

// Let's see out merged data and number of rows
movies.show()
movies.count() //6392  

// Let's remove duplicate and unnecessary columns
val columnsToRemove = Seq("movieId", "tmdbId", "mediaType", "imdb_ratingCount", 
	"smallLensTagCount", "tconst", "nconst", "titleType", "originalTitle", "writers", "startYear", "averageRating", "isAdult", "primaryTitle", "title_lowercase")

val imdbTableDF = movies.drop(columnsToRemove: _*)

// Let's see if all rows are unique -> nope
val uniqueIDs = imdbTableDF.select("imdbId").distinct().count() 

// Let's drop duplicate rows to have only unique rows
val imdbTable= imdbTableDF.dropDuplicates("imdbId")

// Let's see the new final data and it's 
imdbTable.count() //6344

imdbTable.show()

imdbTable.write
  .format("csv") 
  .option("header", "true") 
  .mode("overwrite") 
  .save("moviesFinalData_Cleaned")


// Let's give access to other 2 team members
/*
hdfs dfs -setfacl -m user:jhd9252:rwx /user/pi2018_nyu_edu
hdfs dfs -setfacl -m default:user:jhd9252:rwx /user/pi2018_nyu_edu
hdfs dfs -setfacl -m user:jhd9252_nyu_edu:rwx /user/pi2018_nyu_edu
hdfs dfs -setfacl -m default:user:jhd9252_nyu_edu:rwx /user/pi2018_nyu_edu
hdfs dfs -setfacl -m user:jhd9252:rwx /user/pi2018_nyu_edu/moviesFinalData_Cleaned
hdfs dfs -setfacl -m default:user:jhd9252:rwx /user/pi2018_nyu_edu/moviesFinalData_Cleaned
hdfs dfs -setfacl -m user:jhd9252_nyu_edu:rwx /user/pi2018_nyu_edu/moviesFinalData_Cleaned
hdfs dfs -setfacl -m default:user:jhd9252_nyu_edu:rwx /user/pi2018_nyu_edu/moviesFinalData_Cleaned

hdfs dfs -setfacl -m user:na2794:rwx /user/pi2018_nyu_edu
hdfs dfs -setfacl -m default:user:na2794:rwx /user/pi2018_nyu_edu
hdfs dfs -setfacl -m user:na2794_nyu_edu:rwx /user/pi2018_nyu_edu
hdfs dfs -setfacl -m default:user:na2794_nyu_edu:rwx /user/pi2018_nyu_edu
hdfs dfs -setfacl -m user:na2794:rwx /user/pi2018_nyu_edu/moviesFinalData_Cleaned
hdfs dfs -setfacl -m default:user:na2794:rwx /user/pi2018_nyu_edu/moviesFinalData_Cleaned
hdfs dfs -setfacl -m user:na2794_nyu_edu:rwx /user/pi2018_nyu_edu/moviesFinalData_Cleaned
hdfs dfs -setfacl -m default:user:na2794_nyu_edu:rwx /user/pi2018_nyu_edu/moviesFinalData_Cleaned
*/
