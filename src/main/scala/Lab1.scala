import org.apache.spark.sql.SparkSession
import com.uber.h3core.H3Core
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import scala.sys.process._
import scala.util.{Try, Success, Failure}
import java.text.SimpleDateFormat
import java.util.Date

object Lab1 {
  val geoUDF = udf((lat: Double, lon: Double, res: Int) =>
    h3Helper.toH3func(lat, lon, res)
  )
  val distanceUDF =
    udf((origin: String, des: String) => h3Helper.getH3Distance(origin, des))

  def printConfigs(session: SparkSession) = {
    // get Conf
    val mconf = session.conf.getAll
    //print them
    for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
  }

  def main(args: Array[String]) {
    // ******** Create a SparkSession  ***************

    val spark = SparkSession
      .builder()
      .appName("Lab 1")
      .config("spark.master", "local")
      .config("spark.executor.cores", 4)
      .config("spark.shuffle.file.buffer", "1mb")
      .config("spark.executor.memory", "2g")
      .config("spark.shuffle.unsafe.file.output.buffer", "1mb")
      .config("spark.io.compression.lz4.blockSize", "512kb")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    printConfigs(spark)
    // input check
    val height = typecheck.matchFunc(args(0))
    if (height == 0 || height == -1) {
      println("******************************************************")
      println("Invalid input, program terminated")
      spark.stop
    } else {
      println("******************************************************")
      println("        The sea level rised " + height + " m          ")
      // ************* process osm & alos dataset separately *******************
      val (placeDF, harbourDF) = readOpenStreetMap(
        spark.read.format("orc").load("netherlands-latest.osm.orc")
      ); //complete osm dataset
      val elevationDF = readALOS(spark.read.format("parquet").load("parquet/*")); //complete alos dataset

      // ************** combine two datasets with H3 ************************
      val (floodDF, safeDF) = combineDF(
        placeDF,
        elevationDF,
        height
      )
      // *************** find the closest destination *************
      findClosestDest(floodDF, safeDF, harbourDF)
      // Stop the underlying SparkContext0
      spark.stop

    }

  }
  def readOpenStreetMap(df: DataFrame): (DataFrame, DataFrame) = {
    // ********* explode and filter the useful tags ************
    val splitTagsDF = df
      .select(
        col("id"),
        col("type"),
        col("lat"),
        col("lon"),
        explode(col("tags"))
      )
      .filter(col("type") === "node")
      .filter(
        col("key") === "name" || col("key") === "place" ||
          col("key") === "population" || col("key") === "harbour"
      )
    // ********** make the keys to be column names *************
    val groupdf = splitTagsDF
      .groupBy("id", "type", "lat", "lon")
      // This step causes a shuffle
      // groupBy tranformation is a wide transformation
      // It requires data from other partitions to be read, combined and written to disk
      // Each partition may contain data with the same "id", "type", "lat", "lon" value
      .pivot("key", Seq("name", "place", "population", "harbour"))
      .agg(first("value"))

    // ********** remove the rows with imcomplete information *******
    val groupLessDF = groupdf
      .filter(
        (col("place").isNotNull && col("population").isNotNull &&
          (col("place") === "city" || col("place") === "town" || col(
            "place"
          ) === "village" || col("place") === "halmet")) || col(
          "harbour"
        ) === "yes"
      )

    //********** calculate the coarse/fine-grained H3 value ****************
    val h3mapdf = groupLessDF
      .withColumn("H3", geoUDF(col("lat"), col("lon"), lit(7)))
      //.withColumn("H3Rough",geoUDF(col("lat"),col("lon"),lit(5)))
      .cache() // this is for dividing the places into groups, and the calculation of distances will be done within each groups

    //***********separate the harbours and other places *******************
    val harbourDF = h3mapdf
      .filter(col("harbour") === "yes")
      .select(col("H3").as("harbourH3"))

    val placeDF = h3mapdf
      .select(col("name"), col("population"), col("H3"), col("place"))
      .filter(col("harbour").isNull)
      .dropDuplicates("name") //name is unique

    println("******************************************************")
    println("* Finished building up DAG for reading OpenStreetMap *")

    return (placeDF, harbourDF)

  }
  def readALOS(alosDF: DataFrame): DataFrame = {

    val h3df_raw = alosDF
      .withColumn("H3", geoUDF(col("lat"), col("lon"), lit(7)))
    val h3df = h3df_raw
      .groupBy("H3") //H3 is now unique
      // This step causes a shuffle
      // groupBy tranformation is a wide transformation
      // It requires data from other partitions to be read, combined and written to disk
      // Each partition may contain data with the same H3 value
      .min("elevation")
      .withColumnRenamed("min(elevation)", "elevation")
      .select(col("H3"), col("elevation"))
    println("******************************************************")
    println("**** Finished building up DAG for reading ALOSMap ****")
    
    return h3df
  }
  /*combineDF: combine openstreetmap & alos,
           get the relations: name -> lan,lon
           get flooded, safe df
           get the output orc name | evacuees & sum
   */
  def combineDF(
      df1: DataFrame,
      df2: DataFrame,
      riseMeter: Int
  ): (DataFrame, DataFrame) = {

    /** ****** Combine osm and alos with h3 value *******
      */
    //combinedDF - name,place,population,H3,min(elevation)

    val combinedDF = df1
      .join(df2, Seq("H3"), "inner")
    // This step causes a shuffle
    // The join operation merges two data set over a same matching key
    // It triggers a large amount of data movement across Spark executors

    /** ********split into flood and safe DF **********      */
    //floodDF: place,population, num_evacuees, floodH3
    val floodDF = combinedDF
      .filter(col("elevation") <= riseMeter)
      .drop(
        "elevation",
        "place"
      ) //no need to know the type of flooded place any more
      .withColumnRenamed("population", "num_evacuees")
      .withColumnRenamed("name", "place")
      .withColumnRenamed("H3", "floodH3")
      .withColumn("num_evacuees", col("num_evacuees").cast("int"))
      .cache()
    //	.withColumn("closest_dest",destinationUDF(col("H3")))

    /*
   root
  |-- place: string (nullable = true)
  |-- safeH3: string (nullable = true)
  |-- num_evacuees: integer (nullable = true)
  |
     */
    //safeDF - destination,safeH3,safe_population
    // row satisfied:
    // - safe_place == city | harbour

    val safeDF = combinedDF
      .filter(col("elevation") > riseMeter)
      .drop("elevation")
      .filter(col("place") === "city") //the destination must be a city
      .drop("place")
      .withColumnRenamed("population", "safe_population")
      .withColumnRenamed("name", "destination")
      .withColumnRenamed("H3", "safeH3")
      .cache()

    return (floodDF, safeDF)

  }

  def findClosestDest(
      floodDF: DataFrame,
      safeDF: DataFrame,
      harbourDF: DataFrame
  ) {
    val eva_cities = floodDF
      .crossJoin(safeDF) // find all the possible evacuation destinations
      // This step causes a shuffle
      // The join operation merges two data set over a same matching key
      // It triggers a large amount of data movement across Spark executors
      .withColumn("city_distance", distanceUDF(col("floodH3"), col("safeH3")))
    // eva_cities.show(50,false)

    val min_city = eva_cities
      .groupBy("place")
      // This step causes a shuffle
      // groupBy tranformation is a wide transformation
      // It requires data from other partitions to be read, combined and written to disk
      // Each partition may contain data with the same place value
      .agg(
        min("city_distance").as("city_distance") // find the closest safe city
      )
    // min_city.show(50,false)

    val closest_city = min_city
      .join(
        eva_cities,
        Seq("place", "city_distance")
      ) // join the original dataframe
      // This step causes a shuffle
      // The join operation merges two data set over a same matching key
      // It triggers a large amount of data movement across Spark executors
      .dropDuplicates(
        "place",
        "city_distance"
      ) // avoid duplicate due to the same city_distance
    //println("closest_city")
    // closest_city.show(50,false)

    val closest_harbour = floodDF
      .crossJoin(harbourDF) // find distance to each harbour
      .withColumn(
        "harbour_distance",
        distanceUDF(col("floodH3"), col("harbourH3"))
      )
      .groupBy("place")
      // This step causes a shuffle
      // groupBy tranformation is a wide transformation
      // It requires data from other partitions to be read, combined and written to disk
      // Each partition may contain data with the same place value
      .min("harbour_distance") // choose the cloestest one
      .withColumnRenamed("min(harbour_distance)", "harbour_distance")
    //println("closest_harbour")
    // closest_harbour.show(50,false)
    // join closest_city with closest_harbour based on place name
    val floodToSafe = closest_city
      .join(closest_harbour, Seq("place"), "inner")
      // This step causes a shuffle
      // The join operation merges two data set over a same matching key
      // It triggers a large amount of data movement across Spark executors
      .select(
        "place",
        "num_evacuees",
        "harbour_distance",
        "destination",
        "city_distance",
        "safe_population"
      )
    //println("floodToSafe")
    //floodToSafe.show(50,false)
    /*
      seperate into two dataframes
      |-- near_harbour: places that are closer to a harbour than a safe city
      |-- near_city: places that are closer to a safe city
     */

    //********** divide into 2 DFs ***********
    val near_harbour = floodToSafe
      .filter(col("harbour_distance") <= col("city_distance"))
      .drop("city_distance", "harbour_distance")
    println("******************************************************")
    println("************ places closer to a harbour **************")
    // near_harbour.show(50,false)

    val near_city = floodToSafe
      .filter(col("harbour_distance") > col("city_distance"))
      .drop("harbour_distance", "city_distance")
    println("******************************************************")
    println("************ places closer to a city  ****************")
    // near_city.show(50,false)

    // ********* operation on <near_harbour> DF **********
    val change_dest =
      near_harbour.withColumn(
        "destination",
        lit("Waterworld")
      ) // change the destination
    val change_popu = change_dest
      .withColumn("num_evacuees", col("num_evacuees") * 0.25)
      . // evacuees to the WaterWorld
      withColumn(
        "safe_population",
        col("safe_population") * 0
      ) // set the population of WaterWorld to 0
    val rest_popu = near_harbour.withColumn(
      "num_evacuees",
      col("num_evacuees") * 0.75
    ) // evacuees to the nearest city
    val near_harbour_new =
      rest_popu.union(change_popu).sort("place") // Combined DF
    println("******************************************************")
    println("************ evacuees to harbour and city ************")
    // near_harbour_new.show(50,false) // evacuees to harbour and city

    val relocate_output =
      near_harbour_new
        .union(near_city)
        .sort("place") // Combine <near_harbour_new> and <near_city>

    println("******************************************************")
    println("************* output => evacuees by place ************")
    //relocate_output.show(100,false)
    println("******************************************************")
    println("******************* Saving data **********************")
    //val currTime = new SimpleDateFormat("yyyy-MM-dd-HH:mm").format(new Date)
    relocate_output
      .drop("safe_population")
      .write
      .mode("overwrite")
      .orc("output/data/relocate.orc") // output as .orc file
    //.orc("output/data/relocate" + currTime + ".orc") // output as .orc file
    println("******************* Finished save*********************")
    //val testread1 = spark.read.format("orc").load("output_12.orc")
    //val testread2 = spark.read.format("orc").load("output/data/relocate.orc")
    //val testread3 = spark.read.format("orc").load("output/data/receive_output.orc")

    // ********* calculate the total number of evacuees to each destination ********
    println("******************************************************")
    println("****** aggregate evacuees by their destination *******")
    val receive_popu = relocate_output
      .groupBy("destination")
      // This step causes a shuffle
      // groupBy tranformation is a wide transformation
      // It requires data from other partitions to be read, combined and written to disk
      // Each partition may contain data with the same destination value
      .agg(
        sum("num_evacuees").as("evacuees_received"),
        avg("safe_population").as("old_population")
      );
    /** ******calculate the sum of evacuees*******      */
    println("******************************************************")
    println("********* calculate total number of evacuees *********")
    val sum_popu = receive_popu
      .groupBy()
      // This step causes a shuffle
      // data from different partitions are read
      // to calculate the total number of evacuees
      .sum("evacuees_received")
      .first
      .get(0)

    println("******************************************************")
    println("|        total number of evacuees is " + sum_popu + "        |")
    println("******************************************************")

    // ******* transform the output data into the required format **********
    val receive_output = receive_popu
      .withColumn(
        "new_population",
        col("old_population") + col("evacuees_received")
      )
      .drop("evacuees_received")

    println("******************************************************")
    println("*** output => population change of the destination ***")

    // receive_output.show(10,false)
 
    println("******************************************************")
    println("******************* Saving data **********************")
    receive_output.write
      .mode("overwrite")
      .orc("output/data/receive_output.orc")
    //  .orc("/output/data/receive_output" + currTime + ".orc")
    println("******************* Finished save*********************")

  }

}

object h3Helper {
  val h3 = H3Core.newInstance()
  def toH3func(lat: Double, lon: Double, res: Int): String =
    h3.geoToH3Address(lat, lon, res)

  def getH3Distance(origin: String, des: String): Int = {
    return h3.h3Distance(origin, des)
  }
}
