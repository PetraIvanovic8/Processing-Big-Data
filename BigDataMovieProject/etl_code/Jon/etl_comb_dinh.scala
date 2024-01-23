/* HDFS COMMANDS TO SHARE DATA SOURCES

# Note: This block of commands is for user: pi2018
# Note: Do the same block of code for user: na2794 
hdfs dfs -setfacl -m user:pi2018:rwx /user/jhd9252_nyu_edu
hdfs dfs -setfacl -m default:user:pi2018:rwx /user/jhd9252_nyu_edu
hdfs dfs -setfacl -m user:pi2018_nyu_edu:rwx /user/jhd9252_nyu_edu
hdfs dfs -setfacl -m default:user:pi2018_nyu_edu:rwx /user/jhd9252_nyu_edu
hdfs dfs -setfacl -m user:pi2018:rwx /user/jhd9252_nyu_edu/output_etl_indiv_dinh.csv
hdfs dfs -setfacl -m default:user:pi2018:rwx /user/jhd9252_nyu_edu/output_etl_indiv_dinh.csv
hdfs dfs -setfacl -m user:pi2018_nyu_edu:rwx /user/jhd9252_nyu_edu/output_etl_indiv_dinh.csv
hdfs dfs -setfacl -m default:user:pi2018_nyu_edu:rwx /user/jhd9252_nyu_edu/output_etl_indiv_dinh.csv
*/
  
// scala Team ETL -> Combining data sources & quick cleaning -> done on pi2018's end

// spark-shell --deploy-mode client -i  etl_comb_dinh.scala



// imports
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.{OneHotEncoder, OneHotEncoderModel}

// read in data
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/pi2018_nyu_edu/moviesFinalData/")

// check schema
df.printSchema()

/* Schema before further dropping
root 
 |-- title: string (nullable = true) 
 |-- studio: string (nullable = true)
 |-- lifetime_gross: integer (nullable = true)
 |-- movieId: integer (nullable = true) -drop
 |-- imdbId: string (nullable = true) -drop
 |-- tmdbId: integer (nullable = true) -drop
 |-- mediaType: string (nullable = true)
 |-- imdb_ratingCount: integer (nullable = true) -drop
 |-- imdb_bestRating: integer (nullable = true) 
 |-- imdb_worstRating: integer (nullable = true)
 |-- imdb_averageRating: double (nullable = true)
 |-- contentRating: string (nullable = true)
 |-- smallLensRatingCount: integer (nullable = true) 
 |-- smallLensRatingAvg: double (nullable = true)
 |-- title_lowercase: string (nullable = true)
 |-- genres: string (nullable = true)
 |-- smallLensTagCount: integer (nullable = true) -drop
 |-- releaseYear: integer (nullable = true)
 |-- releaseMonth: integer (nullable = true)
 |-- nconst: string (nullable = true) -drop
 |-- tconst: string (nullable = true) -drop
 |-- titleType: string (nullable = true) -drop
 |-- primaryTitle: string (nullable = true) 
 |-- originalTitle: string (nullable = true) -drop
 |-- numVotes: integer (nullable = true) 
 |-- directors: string (nullable = true)  
 |-- writers: string (nullable = true) -drop
 |-- category: string (nullable = true) 
 |-- primaryName: string (nullable = true) 
 |-- startYear: integer (nullable = true) -drop
 |-- runtimeMinutes: integer (nullable = true)
 |-- averageRating: integer (nullable = true) -drop
 |-- isAdult: integer (nullable = true) -drop

*/

// drop columns
var tmp = df.drop("movieId",
	"imdbId", 
	"tmdbId", 
	"imdbRatingCount",
	"smallLensTagCount",
	"titleType",
	"originalTitle",
	"writers",
	"startYear",
	"averageRating",
	"isAdult",
	"nconst",
	"tconst")

// recheck schema
tmp.printSchema()

/* Updated Schema

-----------Leave out (identifiers)-----------
 |-- title: string (nullable = true)
 |-- primaryTitle: string (nullable = true)
 |-- primaryName: string (nullable = true)
 |-- title_lowercase: string (nullable = true)
 ---------------Normalize--------------------
 |-- lifetime_gross: integer (nullable = true)
 |-- imdb_ratingCount: integer (nullable = true)
 |-- imdb_bestRating: integer (nullable = true)
 |-- imdb_worstRating: integer (nullable = true)
 |-- imdb_averageRating: double (nullable = true)
 |-- smallLensRatingCount: integer (nullable = true)
 |-- smallLensRatingAvg: double (nullable = true)
 |-- numVotes: integer (nullable = true)
 |-- runtimeMinutes: integer (nullable = true)
 ------------Label Encoding-------------------
 |-- releaseYear: integer (nullable = true)
 |-- releaseMonth: integer (nullable = true)
 |-- contentRating: string (nullable = true)
------------One Hot Encoding------------------
 |-- mediaType: string (nullable = true)
 |-- studio: string (nullable = true)
 |-- category: string (nullable = true)
 |-- directors: string (nullable = true)
 |-- genres: string (nullable = true)

*/

// remove duplicates observations
tmp = tmp.distinct()

tmp.coalesce(1).write.option("header",true).csv("output_etl_comb_dinh")





