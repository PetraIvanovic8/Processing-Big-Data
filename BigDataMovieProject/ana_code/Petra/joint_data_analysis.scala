//spark-shell --deploy-mode client

val imdbTable = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/pi2018_nyu_edu/moviesFinalData_Cleaned/")

// Let's look at the relationship between release month for a movie and its rating and gross
val monthlyResults = imdbTable.select("releaseMonth", "lifetime_gross", "imdb_averageRating")
monthlyResults.show()

val monthlyResultsCleaned = monthlyResults.na.drop()
monthlyResultsCleaned.printSchema()

// Calculating the correlation between each pair of columns
val correlationMonthGross = monthlyResultsCleaned.stat.corr("releaseMonth", "lifetime_gross")
val correlationMonthRating = monthlyResultsCleaned.stat.corr("releaseMonth", "imdb_averageRating")
val correlationGrossRating = monthlyResultsCleaned.stat.corr("lifetime_gross", "imdb_averageRating")

// Printing the correlations
println(s"Correlation between month and gross: $correlationMonthGross")
println(s"Correlation between month and rating: $correlationMonthRating")
println(s"Correlation between gross and rating: $correlationGrossRating")

// full table to create correlation matrix for better visualization in seaborn
monthlyResultsCleaned.write
  .format("csv") 
  .option("header", "true") 
  .mode("overwrite") 
  .save("MonthlyResults_Full")

//hdfs dfs -getmerge MonthlyResults_Full /home/pi2018_nyu_edu/monthlyResults.csv

val aggregatedMonthlyResults = monthlyResultsCleaned.groupBy("releaseMonth").agg(
    avg("lifetime_gross").alias("avg_lifetime_gross"),
    expr("percentile_approx(lifetime_gross, 0.5)").alias("median_lifetime_gross"),
    avg("imdb_averageRating").alias("avg_imdb_rating"),
    expr("percentile_approx(imdb_averageRating, 0.5)").alias("median_imdb_rating")
  ).orderBy("releaseMonth")

aggregatedMonthlyResults.show()

aggregatedMonthlyResults.write
  .format("csv") 
  .option("header", "true") 
  .mode("overwrite") 
  .save("MonthlyAverages")

//hdfs dfs -getmerge MonthlyAverages /home/pi2018_nyu_edu/monthlyResults.csv


val studioResults = imdbTable.select("releaseYear", "lifetime_gross", "imdb_averageRating", "studio")
studioResults.show()

val studioResultsCleaned = studioResults.na.drop()

studioResultsCleaned.printSchema()

val uniqueStudios = studioResultsCleaned.select("studio").distinct()
uniqueStudios.count()
uniqueStudios.show(Int.MaxValue)

val uniqueYears = studioResultsCleaned.select("releaseYear").distinct()
uniqueYears.count()



// Group by 'studio', calculate average 'lifetime_gross', and order by it in descending order
val topStudiosByGross = studioResultsCleaned.groupBy("studio").agg(avg("lifetime_gross").alias("average_lifetime_gross")).orderBy(desc("average_lifetime_gross")).limit(20)

// Collect the names of the top 20 studios into a list
val topStudiosList = topStudiosByGross.select("studio").as[String].collect().toList

// Filter the original DataFrame for rows that contain studios in the top 20 list
val filteredStudios = studioResultsCleaned.filter(col("studio").isin(topStudiosList: _*))

// Show the filtered DataFrame - this will contain all rows for the top 20 studios
filteredStudios.show()

filteredStudios.write
  .format("csv") 
  .option("header", "true") 
  .mode("overwrite") 
  .save("topStudios")


//hdfs dfs -getmerge topStudios /home/pi2018_nyu_edu/topStudios.csv







