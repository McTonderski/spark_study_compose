import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder.appName("Online Retail Analysis").getOrCreate()

// Step 1: Reading the Data
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/OnlineRetail.csv")
df.show()

// Step 2: Data Cleaning
val cleanedDF = df.na.drop().dropDuplicates()
cleanedDF.show()

// Step 3: Sales Analysis
val salesDF = cleanedDF.withColumn("TotalSale", col("Quantity") * col("UnitPrice"))
val totalSalesDF = salesDF.groupBy("InvoiceNo").agg(sum("TotalSale").alias("TotalSale"))
totalSalesDF.orderBy(desc("TotalSale")).show()

// Step 4: Product Trends Analysis
val windowSpec = Window.partitionBy("StockCode").orderBy("InvoiceDate")
val salesWithLagDF = salesDF.withColumn("PreviousTotalSale", lag("TotalSale", 1).over(windowSpec))
val salesTrendDF = salesWithLagDF.withColumn("PercentageChange",
  (col("TotalSale") - col("PreviousTotalSale")) / col("PreviousTotalSale") * 100)
salesTrendDF.show()

// Step 5: Geographical Analysis
val countrySalesDF = salesDF.groupBy("Country").agg(sum("TotalSale").alias("TotalSale"))
countrySalesDF.orderBy(desc("TotalSale")).show()

// Save results to CSV
totalSalesDF.write.option("header", "true").csv("/opt/spark-data/output/total_sales")
salesTrendDF.write.option("header", "true").csv("/opt/spark-data/output/sales_trend")
countrySalesDF.write.option("header", "true").csv("/opt/spark-data/output/country_sales")

spark.stop()
