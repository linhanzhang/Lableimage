import org.apache.spark.sql.SparkSession

object Lab1 {
  def main(args: Array[String]) {
    // Create a SparkSession
    val spark =
      SparkSession.builder.appName("Lab 1").getOrCreate

    // ...

    // Stop the underlying SparkContext
    spark.stop
  }
}
