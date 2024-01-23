// to push the data into hdfs after uploading it on hpc we will use these comands:

// hdfs dfs -put name.basics.tsv 
// hdfs dfs -put title.basics.tsv 
// hdfs dfs -put title.crew.tsv 
// hdfs dfs -put title.principals.tsv 
// hdfs dfs -put title.ratings.tsv 


// after putting the data in hdfs this is how we can access it from scala
//spark-shell --deploy-mode client

//load and explore datasets / files 
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

// we will merge this datasets into a main IMDB data in etl step