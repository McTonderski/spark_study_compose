import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder.appName("Online Retail Analysis").getOrCreate()

println("Step 1: Reading the Data")
// Step 1: Reading the Data
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/spark-data/OnlineRetail.csv")
println("Displaying the first 20 rows of the dataset:")
df.show()

println("Step 2: Data Cleaning")
// Step 2: Data Cleaning
println("Checking for missing values and dropping rows with missing values")
val cleanedDF = df.na.drop()
println("Checking for duplicates and dropping duplicate rows")
val deduplicatedDF = cleanedDF.dropDuplicates()
println("Displaying the cleaned data:")
deduplicatedDF.show()

println("Step 3: Sales Analysis")
// Step 3: Sales Analysis
println("Calculating the total sales value for each transaction")
val salesDF = deduplicatedDF.withColumn("TotalSale", col("Quantity") * col("UnitPrice"))
val totalSalesDF = salesDF.groupBy("InvoiceNo").agg(sum("TotalSale").alias("TotalSale"))
println("Displaying the total sales per transaction, sorted by total sales:")
totalSalesDF.orderBy(desc("TotalSale")).show()

println("Step 4: Product Trends Analysis")
// Step 4: Product Trends Analysis
println("Analyzing product sales trends over time")
val windowSpec = Window.partitionBy("StockCode").orderBy("InvoiceDate")
val salesWithLagDF = salesDF.withColumn("PreviousTotalSale", lag("TotalSale", 1).over(windowSpec))
val salesTrendDF = salesWithLagDF.withColumn("PercentageChange",
  (col("TotalSale") - col("PreviousTotalSale")) / col("PreviousTotalSale") * 100)
println("Displaying the product sales trends with percentage changes:")
salesTrendDF.show()

println("Step 5: Geographical Analysis")
// Step 5: Geographical Analysis
println("Calculating total sales by country")
val countrySalesDF = salesDF.groupBy("Country").agg(sum("TotalSale").alias("TotalSale"))
println("Displaying the total sales per country, sorted by total sales:")
countrySalesDF.orderBy(desc("TotalSale")).show()

// Save results to CSV
println("Saving results to CSV files")
totalSalesDF.write.option("header", "true").csv("/opt/spark-data/output/total_sales")
salesTrendDF.write.option("header", "true").csv("/opt/spark-data/output/sales_trend")
countrySalesDF.write.option("header", "true").csv("/opt/spark-data/output/country_sales")

println("Analysis complete. Results saved to /opt/spark-data/output/")
spark.stop()
