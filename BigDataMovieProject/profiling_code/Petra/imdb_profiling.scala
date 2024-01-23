//spark-shell --deploy-mode client

//load the data
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/pi2018_nyu_edu/IMDBdata_joined_raw/")

//confirm all the data is loaded correctly
df.count()
df.show()
df.columns

// Find Means
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.Column

// Mean calculation for numeric columns
val meanValues = df.agg(
  mean("isAdult").alias("mean_isAdult"),
  mean("startYear").alias("mean_startYear"),
  mean("runtimeMinutes").alias("mean_runtimeMinutes"),
  mean("averageRating").alias("mean_averageRating"),
  mean("numVotes").alias("mean_numVotes")
)

meanValues.show()

// Find Medians (50th percentile)

// First lets turn startYear & runtimeMinutes into numerical rather than string columns
val dfNew = df.withColumn("startYearInt", col("startYear").cast("int"))
val df = dfNew.drop("startYear")

val dfNew = df.withColumn("runtimeMinutesInt", col("runtimeMinutes").cast("int"))
var df = dfNew.drop("runtimeMinutes")

df = df.withColumnRenamed("startYearInt", "startYear")
df = df.withColumnRenamed("runtimeMinutesInt", "runtimeMinutes")

df.printSchema()


// Median (50th percentile) calculation for numeric columns
val medianValues = Map(
  "isAdult" -> df.stat.approxQuantile("isAdult", Array(0.5), 0.0).head,
  "startYear" -> df.stat.approxQuantile("startYear", Array(0.5), 0.0).head,
  "runtimeMinutes" -> df.stat.approxQuantile("runtimeMinutes", Array(0.5), 0.0).head,
  "averageRating" -> df.stat.approxQuantile("averageRating", Array(0.5), 0.0).head,
  "numVotes" -> df.stat.approxQuantile("numVotes", Array(0.5), 0.0).head
)

println("Median values")
medianValues.foreach(println)


// Find Modes
// Lets define a function to calculate modes rather than doing it by hand for each value
def calculateMode(df: DataFrame, columnName: String): Any = {
  val modeValue = df.groupBy(columnName)
    .count()
    .orderBy(desc("count"))
    .first()(0)
  modeValue
}

val modeIsAdult = calculateMode(df, "isAdult")
val modeStartYear = calculateMode(df, "startYear")
val modeAverageRating = calculateMode(df, "averageRating")
val modeNumVotes = calculateMode(df, "numVotes")

println(s"Mode for isAdult: $modeIsAdult")
println(s"Mode for startYear: $modeStartYear")
println(s"Mode for averageRating: $modeAverageRating")
println(s"Mode for numVotes: $modeNumVotes")


// Function accounting for nulls (ignoring them) and counting the number of occurances
def calculateModeIgnoringNulls(df: DataFrame, columnName: String): Row = {
  df.filter(col(columnName).isNotNull) // Filter out null values
    .groupBy(columnName)
    .count() // Count occurrences of each value
    .orderBy(desc("count"), col(columnName)) // Order by count descending and then by value to break ties
    .first() // Take the first row, which has the highest count
}

val modeRuntimeMinutes = calculateModeIgnoringNulls(df, "runtimeMinutes")
println(s"Most common runtimeMinutes: ${modeRuntimeMinutes(0)}, Count: ${modeRuntimeMinutes(1)}")


// Find Standard Deviations
val statsDf = df.agg(
  stddev_samp("averageRating").alias("stddev_averageRating"),
  stddev_samp("numVotes").alias("stddev_numVotes"),
  stddev_samp("runtimeMinutes").alias("stddev_runtimeMinutes")
)

statsDf.show()


// Correlation between our numerical columns and outcome of interest - averageRating
val numericalColumns = Array("isAdult", "numVotes", "startYear", "runtimeMinutes")

numericalColumns.foreach { colName =>
  val correlation = df.stat.corr(colName, "averageRating")
  println(s"Correlation between $colName and averageRating is: $correlation")
}

// Print out the number of rows in the table
df.count() 

// Count the number of unique 'directors'
val uniqueDirectorsCount = df.select("directors").distinct().count()

// Count and show unique genres
val uniqueGenres = df.select("genres").distinct() 
println("Unique values in 'genres':")
uniqueGenres.show(truncate = false)
uniqueGenres.count() 

// Count and show unique categories
val uniqueCategories = df.select("category").distinct()
println("Unique values in 'category':")
uniqueCategories.show(truncate = false)

df.write
  .format("csv") 
  .option("header", "true") 
  .mode("overwrite") 
  .save("IMDBdata_joined")
