import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// parsing csv file
val boxoffice= spark.read.option("header",true).csv("cleanboxoffice.csv")

// selecting column to read
val lifetimeGrossCol = boxoffice.select("lifetime_gross")

// creating a new column for comparison for Warner Brothers movies 
val newWBOver7Mil = boxoffice.withColumn("WB_Over_70000000", when(col("lifetime_gross") > 70000000 && col("studio") === "WB", col("title")).otherwise(null))

newWBOver7Mil.show()

val nonNullWB = newWBOver7Mil.select("WB_Over_70000000").na.drop().count()

println("The number of Warner Brothers movies that have a lifetime gross of over $70000000: "+ nonNullWB)

// creating a new column for comparison for Fox Studios movies 
val newFoxOver7Mil = boxoffice.withColumn("Fox_Over_70000000", when(col("lifetime_gross") > 70000000 && col("studio") === "Fox", col("title")).otherwise(null))

newFoxOver7Mil.show()

val nonNullFox = newFoxOver7Mil.select("Fox_Over_70000000").na.drop().count()

println("The number of Fox movies that have a lifetime gross of over $70000000: "+ nonNullFox)

// creating a new column for comparison for Paramount movies 
val newParOver7Mil = boxoffice.withColumn("Par_Over_70000000", when(col("lifetime_gross") > 70000000 && col("studio") === "Par.", col("title")).otherwise(null))

newParOver7Mil.show()

val nonNullPar = newParOver7Mil.select("Par_Over_70000000").na.drop().count()

println("The number of Paramount movies that have a lifetime gross of over $70000000: "+ nonNullPar)
