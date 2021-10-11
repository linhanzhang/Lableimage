import org.apache.spark.sql.SparkSession
import com.uber.h3core.H3Core
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import scala.sys.process._
import java.text.SimpleDateFormat
import java.util.Date

object Lab1 {
  val geoUDF = udf((lat: Double, lon: Double, res: Int) =>
    h3Helper.toH3func(lat, lon, res)
  )
   val distanceUDF = udf((origin:String,des:String) => h3Helper.getH3Distance(origin,des))
  // val destinationUDF = udf((origin:String,des:String) => h3Helper.getClosestCity(origin:String,des:String))
  // val findClosestCity =
  //   udf((origin: String, des: Array[(String, String, Int)]) =>
  //     h3Helper.getClosestDest(origin, des)
  //   )
  //return "dd"
  // val findClosestHarbour = udf((origin: String, des: Array[String]) =>
  //   h3Helper.getClosestDest(origin, des)
  // )

  def main(args: Array[String]) {
    // ******** Create a SparkSession  ***************
    val spark = SparkSession
      .builder()
      .appName("Lab 1")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", 8)
    // ************* process osm & alos dataset separately *******************
     val (df1, harbourDF) = readOpenStreetMap(
       spark.read.format("orc").load("utrecht-latest.osm.orc")
     ); // Utrecht dataset - corresponds to N052E005
     val df2 = readALOS(
       spark.read.load("parquet/ALPSMLC30_N052E005_DSM.parquet")
     ); //Utrecht partial alos dataset
    // val (df1,harbourDF)=readOpenStreetMap(spark.read.format("orc").load("zuid-holland-latest.osm.orc")); //zuid-holland dataset - corresponds to N052E004
    // val df2=readALOS(spark.read.load("parquet/ALPSMLC30_N052E004_DSM.parquet")); //partial alos dataset
    //  val (df1, harbourDF) = readOpenStreetMap(spark.read.format("orc").load("netherlands-latest.osm.orc")); //complete osm dataset
    //  val df2 = readALOS(spark.read.load("parquet/*")); //complete alos dataset

    // ************** combine two datasets with H3 ************************
    val (floodDF, safeDF) = combineDF(
      df1.select(col("name"), col("population"), col("H3"), col("place")),
      df2.select(col("H3"), col("elevation")),
      args(0).toInt
    )

    // *************** find the closest destination *************
    findClosestDest(floodDF, safeDF, harbourDF)
    // Stop the underlying SparkContext0
    spark.stop
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
      .filter(
        col("key") === "name" || col("key") === "place" ||
          col("key") === "population" || col("key") === "harbour"
      )
    // ********** make the keys to be column names *************
    val groupdf = splitTagsDF
      .groupBy("id", "type", "lat", "lon")
      .pivot("key", Seq("name", "place", "population", "harbour"))
      .agg(first("value"))
    /*
	  root
	  |-- id: long (nullable = true)
	  |-- type: string (nullable = true)
	  |-- lat: decimal(9,7) (nullable = true)
	  |-- lon: decimal(10,7) (nullable = true)
	  |-- name: string (nullable = true)
	  |-- place: string (nullable = true)
	  |-- population: string (nullable = true)
	  |-- harbour: string (nullable = true)
+-------+--------+----+----+--------------+-----+----------+-------+
|id     |type    |lat |lon |name          |place|population|harbour|
+-------+--------+----+----+--------------+-----+----------+-------+
|144640 |relation|null|null|Hooglanderveen|null |null      |null   |
|333291 |relation|null|null|Bus 73: Maarss|null |null      |null   |
|358048 |relation|null|null|Bus 102: Utrec|null |null      |null   |
     */

    // ********** remove the rows with imcomplete information *******
    val groupLessDF = groupdf
      .filter(col("type") === "node")
      .filter(
        (col("place").isNotNull && col("population").isNotNull &&
          (col("place") === "city" || col("place") === "town" || col(
            "place"
          ) === "village" || col("place") === "halmet")) || col(
          "harbour"
        ) === "yes"
      )

    /*
   +----------+----+----------+---------+-----------+-------+----------+-------+
|			      groupLessDF data 	   			       |
+----------+----+----------+---------+-----------+-------+----------+-------+
|id        |type|lat       |lon      |name       |place  |population|harbour|
+----------+----+----------+---------+-----------+-------+----------+-------+
|44843991  |node|52.0102642|5.4332757|Leersum    |village|7511      |null   |
|44710922  |node|51.9810496|5.1220284|Hagestein  |village|1455      |null   |
|994008023 |node|52.0367152|5.0836117|Nieuwegein |town   |61869     |null   |
|44701792  |node|51.9791304|4.8574054|Polsbroek  |village|1199      |null   |
|5435880522|node|52.2452013|5.3731599|Bunschoten |town   |21866     |null   |
|947021601 |node|52.2177783|4.9372124|Zeilschool |null   |null      |yes    | */

    //********** calculate the coarse/fine-grained H3 value ****************
    val h3mapdf = groupLessDF
      .withColumn("H3", geoUDF(col("lat"), col("lon"), lit(7)))
      //.withColumn("H3Rough",geoUDF(col("lat"),col("lon"),lit(5)))
      .cache() // this is for dividing the places into groups, and the calculation of distances will be done within each groups

    /*
 +-------+----+------+-----+---------+-------+------+-------+---------------+--------------+
 |   					h3mapdf data 					      | +-------+----+------+-----+---------+-------+------+-------+---------------+---------------+
 |id     |type|lat   |lon  |name     |place |popu  |harbour |     H3        |H3Rough x       |
 +-------+----+------+-----+---------+-------+------+-------+---------------+---------------+
 |4484399|node|52.010|5.433|Leersum  |village|7511  |null   |8a1969053247fff|85196907fffffff|
 |4471092|node|51.981|5.122|Hagestein|village|1455  |null   |8a196972e56ffff|85196973fffffff|
 |4556876|node|52.174|5.290|Soest    |town   |39395 |null   |8a19691890a7fff|8519691bfffffff|
 |9661556|node|52.116|4.835|Zegveld  |village|2310  |null   |8a196940980ffff|85196943fffffff|
     */

    //***********separate the harbours and other places *******************
    val harbourDF = h3mapdf
      .filter(col("harbour") === "yes")
      .select(col("H3").as("harbourH3"))
      .cache()

    val placeDF = h3mapdf
      .filter(col("harbour").isNull)
      .drop("harbour")
      .dropDuplicates("name") //name is unique
      .cache()

    println("******************************************************")
    println("* Finished building up DAG for reading OpenStreetMap *")

    return (placeDF, harbourDF)
    /*
+----------+----+----------+---------+--------+-------+-----+---------------+---------------+
|id        |type|lat       |lon      |name    |place  |popu |H3             |H3Rough        |
+----------+----+----------+---------+--------+-------+-----+---------------+---------------+
|44843991  |node|52.0102642|5.4332757|Leersum |village|7511 |8a1969053247fff|85196907fffffff|
|44710922  |node|51.9810496|5.1220284|Hageste |village|1455 |8a196972e56ffff|85196973fffffff|
|45568761  |node|52.1746100|5.2909500|Soest   |town   |39395|8a19691890a7fff|8519691bfffffff|
     */
  }
  def readALOS(alosDF: DataFrame): DataFrame = {

    val h3df = alosDF
      .withColumn("H3", geoUDF(col("lat"), col("lon"), lit(7)))
      .groupBy("H3") //H3 is now unique
      .min("elevation")
      .withColumnRenamed("min(elevation)", "elevation")
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

    /******** Combine osm and alos with h3 value ********/
    //combinedDF - name,place,population,H3,min(elevation)

    val combinedDF = df1
      .join(df2, Seq("H3"), "inner")

    /* val combinedDF=combinedDF_pre
    	.join(combineMinDF,Seq("name","elevation"))
    	.dropDuplicates("name") */

    // combinedDF.filter(col("name")==="Den Haag").show(10)

    //  sys.exit(0)
    /** ********split into flood and safe df **********
      */

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
    // val closest_city = floodDF. crossJoin(safeDF)// find all the possible evacuation destinations
    //     .withColumn("city_distance",distanceUDF(col("floodH3"),col("safeH3")))
    //     .groupBy("place")
    //     .agg(
    //     min("city_distance").as("city_distance"), // find the closest safe city
    //     first("destination").as("destination"),
    //     first("safe_population").as("safe_population")
    //     )

    //closest_city.show(50,false)
// +-------------------+-------------+-----------+---------------+
// |place              |city_distance|destination|safe_population|
// +-------------------+-------------+-----------+---------------+
// |Eemdijk            |7            |Amersfoort |139259         |
// |Loenen aan de Vecht|7            |Amersfoort |139259         |
// |Vreeland           |8            |Amersfoort |139259         |
// |Hoogland           |2            |Amersfoort |139259         |
// +-------------------+-------------+-----------+---------------+
val closest_harbour = floodDF. crossJoin(harbourDF)// find distance to each harbour
    .withColumn("harbour_distance",distanceUDF(col("floodH3"),col("harbourH3")))
    .groupBy("place")
    .min("harbour_distance") // choose the cloestest one
    .withColumnRenamed("min(harbour_distance)","harbour_distance")
    //  closest_harbour.show(50,false)
val 
    //+----------+------------+-----------+----------+---------------+--------+
    //|place     |num_evacuees|destination||safe_population|distance|  floodH3
    //+----------+------------+-----------+----------+---------------+--------+
    //|Bleiswijk |11919       |Delft      |city      |101386         |101     |
    //|Nootdorp  |19160       |Delft      |city      |101386         |35      |

    /*= val floodToSafe=floodDF   //join flood & safe df with H3Rough, calculate the distance between each place and destination
     .join(safeDF,Seq("H3Rough"),"inner")
     .withColumn("city_distance",distanceUDF(col("floodH3"),col("safeH3")))
     .drop("safeH3")
     .cache() */

    // val listFlood=floodDF.select("place").rdd.map(r=>r.getString(0)).collect
    //val safeMap=safeDF.rdd.map(row => (row.getString(1) -> row.getString(0))).collectAsMap()
    // val safeMap = safeDF.rdd
    //   .map(x => (x.get(1).toString, x.get(0).toString, x.get(2).toString.toInt))
    //   .collect()
    // val safeHarbour = harbourDF.rdd.map(r => r.get(0).toString).collect()
    // println("safeMap:")
    
    //   println(safeHarbour)
    //   println(lit(safeHarbour))

    // val test=typedLit(safeMap)
    //  println(test)
    // place,floodH3,num_evacuees,destination,city_distance,safe_population,harbour_distance
    // val arr=Array("8a1969623707fff","8a1fa4926007fff")
 

// +-------------+---------------+----------+------------+---------------+-----------+---------------+
// |city_distance|H3Rough        |place     |num_evacuees|floodH3        |destination|safe_population|
// +-------------+---------------+----------+------------+---------------+-----------+---------------+
// |101          |83196bfffffffff|Bleiswijk |11919       |8a196bb2e347fff|Delft      |101386         |
// |28           |83196bfffffffff|Oegstgeest|23608       |8a19694b2417fff|Leiden     |123753         |

    /*  println("******************************************************")
     println("*************** find the closest city ****************")
     val closestDest=floodToSafe //find the closest city for each flooded place, in "closestDest" each place is distinct
       .join(floodToSafe.groupBy("place")
       .min("city_distance")
       .withColumnRenamed("min(city_distance)","city_distance")
       ,Seq("city_distance","place")
       )





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
    println("************ cities closer to a harbour **************")

    //near_harbour.show(5,false) // close to harbour
    /*
     	+-----+------------+-----------+---------------+
	|place|num_evacuees|destination|safe_population|
	+-----+------------+-----------+---------------+
	|B    |100         |C137       |1000           |
	|C    |100         |C137       |1000           |
	+-----+------------+-----------+---------------+

     */

    val near_city = floodToSafe
      .filter(col("harbour_distance") > col("city_distance"))
      .drop("harbour_distance", "city_distance")

    println("******************************************************")
    println("************ cities closer to a city  ****************")

    //near_city.show(5,false) // close to city
    /*
     	+-----+------------+-----------+---------------+
	|place|num_evacuees|destination|safe_population|
	+-----+------------+-----------+---------------+
	|A    |100         |C137       |1000           |
	|D    |100         |C137       |1000           |
	|E    |100         |C137       |1000           |
	+-----+------------+-----------+---------------+

     */

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
    //near_harbour_new.show(50,false) // evacuees to harbour and city
    /*
     	+-----+------------+-----------+---------------+
	|place|num_evacuees|destination|safe_population|
	+-----+------------+-----------+---------------+
	|B    |75.0        |C137       |1000           |
	|B    |25.0        |Waterworld |0              |
	|C    |75.0        |C137       |1000           |
	|C    |25.0        |Waterworld |0              |
	+-----+------------+-----------+---------------+
     */

    val relocate_output =
      near_harbour_new
        .union(near_city)
        .sort("place") // Combine <near_harbour_new> and <near_city>

    println("******************************************************")
    println("************* output => evacuees by place ************")

    //relocate_output.show(50,false)
    /*
     	+-----+------------+-----------+---------------+
	|place|num_evacuees|destination|safe_population|
	+-----+------------+-----------+---------------+
	|A    |100.0       |C137       |1000           |
	|B    |25.0        |Waterworld |0              |
	|B    |75.0        |C137       |1000           |
	|C    |75.0        |C137       |1000           |
	|C    |25.0        |Waterworld |0              |
	|D    |100.0       |C137       |1000           |
	|E    |100.0       |C137       |1000           |
	+-----+------------+-----------+---------------+

     */

    // println("******************************************************")
    // println("******************* Saving data **********************")
    // val currTime = new SimpleDateFormat("yyyy-MM-dd-HH:mm").format(new Date)
    // relocate_output
    //   .drop("safe_population")
    //   .write
    //   .mode("overwrite")
    //   .orc("output/data/relocate" + currTime + ".orc") // output as .orc file
    // println("******************* Finished save*********************")

    /*
     val output_12 = spark.createDataFrame(spark.sparkContent.parallelize(relocate_output),schema) //re-create data with the required schema
     output_12.write.orc("relocate_output_12.orc")

     val testread = spark.read.format("orc").load("output_12.orc")
     */

    // ********* calculate the total number of evacuees to each destination ********
    println("******************************************************")
    println("****** aggregate evacuees by their destination *******")
    val receive_popu = relocate_output
      .groupBy("destination")
      .agg(
        sum("num_evacuees").as("evacuees_received"),
        avg("safe_population").as("old_population")
      );
    /*
     	+-----------+-----------------+--------------+
	|destination|evacuees_received|old_population|
	+-----------+-----------------+--------------+
	|Waterworld |50.0             |0.0           |
	|C137       |450.0            |1000.0        |
	+-----------+-----------------+--------------+
     */

    /** ******calculate the sum of evacuees*******
      */

    println("******************************************************")
    println("********* calculate total number of evacuees *********")
    // val sum_popu = receive_popu
    //   .groupBy()
    //   .agg(sum("evacuees_received"))
    //   .first
    //   .get(0)

    println("******************************************************")
   // println("|        total number of evacuees is " + sum_popu + "        |")
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

    //receive_output.show(50,false)
    /*
	+-----------+--------------+--------------+
	|destination|old_population|new_population|
	+-----------+--------------+--------------+
	|Waterworld |0.0           |50.0          |
	|C137       |1000.0        |1450.0        |
	+-----------+--------------+--------------+

     */

    // println("******************************************************")
    // println("******************* Saving data **********************")
    // receive_output.write
    //   .mode("overwrite")
    //   .orc("/output/data/receive_output" + currTime + ".orc")
    // println("******************* Finished save*********************")

  }

}

object h3Helper {
  val h3 = H3Core.newInstance()
  def toH3func(lat: Double, lon: Double, res: Int): String =
    h3.geoToH3Address(lat, lon, res)

  def getH3Distance(origin:String,des:String):Int ={
   	return h3.h3Distance(origin,des)
  } 

  def getClosestDest(
      origin: String,
      safeMap: Array[(String, String, Int)]
  ): (String, Int, Int) = {
    // println("input safemap")
    //h3, name , safepopu
    // println(safeMap)
    for (k <- 1 to 200) {

      val neighbours = h3.kRing(origin, k)
      for ((safeH3, place, safe_population) <- safeMap) {
        //println(safeH3)
        if (neighbours.contains(safeH3)) { //means that the city is near the ori
          println(k)
          return (place, h3.h3Distance(origin, safeH3), safe_population.toInt)
        }
      }
    }

    return ("no such city", 0, 9999)

  }

  def getClosestDest(origin: String, harbours: Array[String]): Int = {
    for (k <- 1 to 50) {
      val neighbours = h3.kRing(origin, k)
      for (harbour <- harbours) {
        if (neighbours.contains(harbour)) { //means that the city is near the ori
          return h3.h3Distance(origin, harbour)
        }
      }
    }
    return 9999

  }

}
