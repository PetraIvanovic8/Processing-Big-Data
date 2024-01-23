import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// parsing csv file
val boxoffice= spark.read.option("header",true).csv("cleanboxoffice.csv")

