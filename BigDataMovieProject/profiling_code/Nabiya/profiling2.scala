import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//parsing csv file
val boxoffice = spark.read.option("header", true).csv("cleanboxoffice.csv")

//selecting column to read and casting it as double
val boxofficeD = boxoffice.withColumn("lifetime_gross", col("lifetime_gross").cast(DoubleType))

//calculating mean of lifetime_gross
val count = boxofficeD.count()
val sumLG = boxofficeD.agg(sum("lifetime_gross")).first.getDouble(0)
val mean = sumLG / count

//finding median of lifetime_gross
val median = boxofficeD.stat.approxQuantile("lifetime_gross", Array(0.5), 0.01)(0)

//finding mode of lifetime_gross
val mode = boxofficeD.groupBy("lifetime_gross").count().orderBy(desc("count")).select("lifetime_gross").limit(1).collect()(0)(0)

//Ccalculating standard deviation
//1
val stdDev = boxofficeD.select(stddev("lifetime_gross").alias("standard_deviation")).first.getDouble(0)

//2
val variance = boxofficeD.agg(avg("lifetime_gross"), var_samp("lifetime_gross")).first.getDouble(1)
val stdDev2 = math.sqrt(variance)

//making studio names lowercase
val boxofficeLower = boxofficeD.withColumn("studio_lower", lower(col("studio")))
boxofficeLower.show()

//creating a new column for comparison for Warner Brothers movies 
val boxofficeNewCol = boxofficeD.withColumn("WB + Over70000000", when(col("lifetime_gross") > 70000000 && col("studio") === "WB", col("title")).otherwise(null))
boxofficeNewCol.show()
