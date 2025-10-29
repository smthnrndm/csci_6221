import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.knowm.xchart.{CategoryChartBuilder, BitmapEncoder}
import org.knowm.xchart.PieChart
import org.knowm.xchart.PieChartBuilder


object ReadCSV {
  def main(args: Array[String]): Unit = {
    System.setProperty("java.awt.headless", "true")

    val spark = SparkSession.builder()
      .appName("Read CSV Example")
      .master("local[*]")
      .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("mental_health_dataset.csv")

    // Filter out rows where employment_status contains "unemployment"
    val cleanedDF = df.filter(!lower(col("employment_status")).contains("unemployed"))

    //student DF
    val studentDF = df.filter(lower(col("employment_status")).contains("student"))

    //employed DF
    val employedDF = cleanedDF.filter(lower(col("employment_status")).contains("employed") || lower(col("employment_status")).contains("self-employed"))

    //show Dfs    
    studentDF.show(5)
    employedDF.show(5)

    // =======================
    // Pie chart: Gender distribution in studentDF
    // =======================
    // Count occurrences of each gender
    val genderCounts = employedDF.groupBy("gender").count().collect()

    // Convert to arrays/lists for XChart
    val genders = genderCounts.map(_.getString(0)).toList
    val counts = genderCounts.map(_.getLong(1).toDouble).toList

    // Create Pie Chart
    val pieChart = new PieChartBuilder()
      .width(600)
      .height(400)
      .title("Gender Distribution of Workers")
      .build()

    for (i <- genders.indices) {
      pieChart.addSeries(genders(i), counts(i))
    }

    // Save chart as PNG
    BitmapEncoder.saveBitmap(pieChart, "worker_gender_pie.png", BitmapEncoder.BitmapFormat.PNG)
    println("Pie chart saved as worker_gender_pie.png")

    spark.stop()
  }
}
