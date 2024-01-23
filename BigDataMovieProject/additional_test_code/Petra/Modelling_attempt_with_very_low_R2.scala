//spark-shell --deploy-mode client

val df = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs:///user/pi2018_nyu_edu/IMDBdata_joined/")

val cleanDf = df.dropDuplicates("tconst") // keep only unique titles

val df = cleanDf

//confirm all the data is loaded correctly
df.count() //3126720
df.show()
df.columns

//let's check if any of our columns have a lot of NA values
var workingdf = df.na.replace(df.columns, Map("\\N" -> null)) //to cound \N as null

val columns = workingdf.columns

val naCounts = columns.map(column => {
    val naCount = workingdf.filter(workingdf(column).isNull || workingdf(column) === "" || workingdf(column).isNaN).count()
    (column, naCount)
})

naCounts.foreach { case (column, count) =>
    println(s"Column: $column, NA Count: $count")
}

// Unique jobs
val distinctJobs = workingdf.select("job").distinct()
distinctJobs.show()

// Let's drop the job column
workingdf = df.drop("job")
workingdf.columns


// Let's standarize numerical columns - averageRating, numVotes, startYear, runtimeMinutes

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler, Imputer}
import org.apache.spark.ml.linalg.Vectors


// Fill the nulls in  numerical with median values 

val colsToStandardize = Array("averageRating", "numVotes", "startYear", "runtimeMinutes")

val imputer = new Imputer()
  .setInputCols(colsToStandardize)
  .setOutputCols(colsToStandardize.map(c => s"${c}_imputed"))
  .setStrategy("median")

val dfWithImputed = imputer.fit(workingdf).transform(workingdf)

// Creating a VectorAssembler to combine the columns into a single vector column
val assembler = new VectorAssembler()
  .setInputCols(colsToStandardize.map(c => s"${c}_imputed"))
  .setOutputCol("features")

// Transform the data to include the features vector column
val dfWithFeatures = assembler.transform(dfWithImputed)

// Define the StandardScaler model
val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithStd(true)  // to scale to unit standard deviation
  .setWithMean(true) // to center the data before scaling


// Fit the StandardScaler model on the data
val scalerModel = scaler.fit(dfWithFeatures)

// Transform the data to include the scaled features
val scaledData = scalerModel.transform(dfWithFeatures)


// Extract the scaled values into separate columns
val toDouble = udf((v: Vector) => v.toArray)
val dfWithScaledCols = scaledData.withColumn("scaled", toDouble(col("scaledFeatures")))

val scaledCols = colsToStandardize.map(c => s"scaled_${c}")
val finalDf = scaledCols.zipWithIndex.foldLeft(dfWithScaledCols) { (df, colNameIndex) =>
  val (colName, idx) = colNameIndex
  df.withColumn(colName, col("scaled")(idx))
}

// Drop the intermediate 'features' and 'scaled' vector columns
val resultDf = finalDf.drop("features", "scaledFeatures", "scaled")

// Show the result
resultDf.show()


// Let's create one-hot encoding for categorical column 'category' and MultiLabel Binarizer for 'genre'


// Let's see unique values for the "genres" column
val uniqueGenres = resultDf.select("genres").distinct() 
println("Unique values in 'genres':")
uniqueGenres.show(truncate = false)
uniqueGenres.count() // too many to use one-hot encoding

// Let's use MultiLabel Binarizer to encode 'genres' column
import org.apache.spark.ml.feature.{CountVectorizer}

// Split the genre string into an array of genres
val withGenreArray = resultDf.withColumn("genreArray", split(col("genres"), ",\\s*"))

// Use CountVectorizer to convert the array of genre tokens into a feature vector
val vectorizer = new CountVectorizer()
  .setInputCol("genreArray")
  .setOutputCol("genreFeatures")
  .setBinary(true)

val genreFeatureDf = vectorizer.fit(withGenreArray).transform(withGenreArray)

genreFeatureDf.show(truncate = false)


// Let's see unique values for the "category" column
val uniqueCategories = resultDf.select("category").distinct()
println("Unique values in 'category':")
uniqueCategories.show(truncate = false)

// Let's use one-hot encoding to encode 'category' column
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder}

val resultDf = genreFeatureDf

// Let's fill the nulls with 'unknown'
val filledDf = resultDf.na.fill("unknown", Seq("category"))

// Create a StringIndexer
val indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIndex")

// Fit the StringIndexer to the DataFrame, which returns a StringIndexerModel
val indexerModel = indexer.fit(filledDf)

// Now use the StringIndexerModel to transform the DataFrame
val indexedDf = indexerModel.transform(filledDf)

//indexedDf.show()

import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, OneHotEncoderModel}


// Apply one-hot encoding
val encoder = new OneHotEncoder()
  .setInputCol("categoryIndex")
  .setOutputCol("categoryVec")

val encoderModel = encoder.fit(indexedDf)
val encodedDf = encoderModel.transform(indexedDf)

//encodedDf.show()

// Show the resulting DataFrame with the one-hot encoded category
encodedDf.select("category", "categoryVec").show(truncate = false)


// Lets create embeddings for word features - 'writers', 'primaryName', 'directors'
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.ml.feature.{Tokenizer, Word2Vec, VectorAssembler, OneHotEncoder, StringIndexer, StandardScaler, Imputer, CountVectorizer}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors

import org.apache.spark.ml.feature.Tokenizer


// Replace null values with an empty string for the text columns before tokenizing
val filledDf = encodedDf.na.fill("", Seq("writers", "primaryName", "directors"))

val encodedDf = filledDf

// Let's create the embeddings using Word2Vec
val writersTokenizer = new Tokenizer().setInputCol("writers").setOutputCol("writersTokens")
val primaryNameTokenizer = new Tokenizer().setInputCol("primaryName").setOutputCol("primaryNameTokens")
val directorsTokenizer = new Tokenizer().setInputCol("directors").setOutputCol("directorsTokens")


val writersWord2Vec = new Word2Vec()
  .setInputCol("writersTokens")
  .setOutputCol("writersFeatures")
  .setVectorSize(50) 

val primaryNameWord2Vec = new Word2Vec()
  .setInputCol("primaryNameTokens")
  .setOutputCol("primaryNameFeatures")
  .setVectorSize(50) 

val directorsWord2Vec = new Word2Vec()
  .setInputCol("directorsTokens")
  .setOutputCol("directorsFeatures")
  .setVectorSize(50)

// Create the VectorAssembler to combine feature columns into a single vector
val assembler = new VectorAssembler()
  .setInputCols(Array( "scaled_numVotes", "scaled_startYear", "scaled_runtimeMinutes", "categoryVec", "genreFeatures", "writersFeatures", "primaryNameFeatures", "directorsFeatures"))
  .setOutputCol("features")


// Split the data into training and testing sets
val Array(trainingData, testData) = encodedDf.randomSplit(Array(0.8, 0.2))

// Define the RandomForestRegressor
val rf = new RandomForestRegressor()
  .setLabelCol("averageRating")
  .setFeaturesCol("features")

// Create the Pipeline with all stages
val pipeline = new Pipeline()
  .setStages(Array(writersTokenizer, primaryNameTokenizer, directorsTokenizer, writersWord2Vec, primaryNameWord2Vec, directorsWord2Vec, assembler, rf))

// Fit the Pipeline to the training data
val model = pipeline.fit(trainingData)


// Make predictions on the test data
val predictions = model.transform(testData)

// Create an instance of RegressionEvaluator specifying the label and prediction columns
val evaluator = new RegressionEvaluator()
  .setLabelCol("averageRating")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

// Evaluate the predictions to calculate the RMSE
val rmse = evaluator.evaluate(predictions)

// Print out the RMSE to understand the error magnitude
println(s"Root Mean Squared Error: $rmse")



import org.apache.spark.ml.regression.LinearRegression

// Create the VectorAssembler to combine feature columns into a single vector
val assembler = new VectorAssembler()
  .setInputCols(Array("scaled_numVotes", "scaled_startYear", "scaled_runtimeMinutes", "categoryVec", "genreFeatures", "writersFeatures", "primaryNameFeatures", "directorsFeatures"))
  .setOutputCol("features")

// Split the data into training and testing sets
val Array(trainingData, testData) = encodedDf.randomSplit(Array(0.8, 0.2))

// Define the LinearRegression estimator
val lr = new LinearRegression()
  .setLabelCol("averageRating")
  .setFeaturesCol("features")

// Create the Pipeline with all stages
val pipeline = new Pipeline()
  .setStages(Array(writersTokenizer, primaryNameTokenizer, directorsTokenizer, writersWord2Vec, primaryNameWord2Vec, directorsWord2Vec, assembler, lr))

// Fit the Pipeline to the training data
val model = pipeline.fit(trainingData)

// Make predictions on the test data
val predictions = model.transform(testData)

// Create an instance of RegressionEvaluator specifying the label, prediction columns, and metric
val r2Evaluator = new RegressionEvaluator()
  .setLabelCol("averageRating")
  .setPredictionCol("prediction")
  .setMetricName("r2")

// Evaluate the predictions to calculate R^2
val r2 = r2Evaluator.evaluate(predictions)

// Print out the R^2 to understand the proportion of variance explained by the model
println(s"Coefficient of Determination (R^2): $r2")


val rmseEvaluator = new RegressionEvaluator()
  .setLabelCol("averageRating")
  .setPredictionCol("prediction")
  .setMetricName("rmse")

// Evaluate the predictions to calculate RMSE
val rmse = rmseEvaluator.evaluate(predictions)

// Print out the RMSE to understand the proportion of variance explained by the model
println(s"Root Mean Squared Error: $rmse")










