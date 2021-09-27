import org.apache.spark.sql.SparkSession
import com.uber.h3core.H3Core
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import scala.sys.process._ 

object Lab1 {

  val geoUDF = udf((lat: Double, lon:Double, res: Int) => h3Helper.toH3func(lat,lon,res))
  
  val distanceUDF = udf((origin:String,des:String) => h3Helper.getH3Distance(origin,des))

  def main(args: Array[String]) {
  
    // ******** Create a SparkSession  ***************
    val spark = SparkSession
        .builder()
        .appName("Lab 1")
        .config("spark.master", "local")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    // ************* process osm & alos dataset separately *******************
    //val (df1,harbourDF)=readOpenStreetMap(spark.read.format("orc").load("utrecht-latest.osm.orc"));// Utrecht dataset - corresponds to N052E005  
    //val df2=readALOS(spark.read.load("parquet/ALPSMLC30_N052E005_DSM.parquet")); //Utrecht partial alos dataset
   // val (df1,harbourDF)=readOpenStreetMap(spark.read.format("orc").load("zuid-holland-latest.osm.orc")); //zuid-holland dataset - corresponds to N052E004
   // val df2=readALOS(spark.read.load("parquet/ALPSMLC30_N052E004_DSM.parquet")); //partial alos dataset
     val (df1,harbourDF)=readOpenStreetMap(spark.read.format("orc").load("netherlands-latest.osm.orc")); //complete osm dataset
     val df2=readALOS(spark.read.load("parquet/*"));    //complete alos dataset 
    
    
    // ************** combine two datasets with H3 ************************
    val (floodDF, safeDF)=combineDF(df1.select(col("name"),col("population"),col("H3"),col("place"),col("H3Rough")),
    df2.select(col("H3"),col("elevation")),
    args(0).toInt)
    
    // *************** find the closest destination *************
    findClosestDest(floodDF,safeDF,harbourDF)
    
    // Stop the underlying SparkContext
    spark.stop
  }
  

  def readOpenStreetMap(df:DataFrame) : (DataFrame,DataFrame) = {
  
   // ********* explode and filter the useful tags ************
    val splitTagsDF=df
      .select(col("id"),col("type"),col("lat"),col("lon"),explode(col("tags")))
      .filter(col("key") ==="name" || col("key") === "place" || col("key") === "population" || col("key") === "harbour")
   
   
   // ********** make the keys to be column names *************
    val groupdf=splitTagsDF
      .groupBy("id","type","lat","lon")
      .pivot("key", Seq("name", "place", "population","harbour"))
      .agg(first("value"))
    
    // ********** remove the rows with imcomplete information *******
    val groupLessDF=groupdf
     .filter(col("type") === "node")
     .filter((col("place").isNotNull && col("population").isNotNull && 
    (col("place") ==="city" || col("place") ==="town" ||col("place")==="village" || col("place") ==="halmet" )) || col("harbour") === "yes" )
    
   
    //********** calculate the coarse/fine-grained H3 value ****************
    val h3mapdf=groupLessDF.withColumn("H3",geoUDF(col("lat"),col("lon"),lit(10)))
    .withColumn("H3Rough",geoUDF(col("lat"),col("lon"),lit(5))); // this is for dividing the places into groups, and the calculation of distances will be done within each groups
    
    
    //***********separate the harbours and other places *******************
    val harbourDF=h3mapdf
    	.filter(col("harbour") === "yes" )
    	.select(col("H3").as("harbourH3"),col("H3Rough"))
    	.cache()
    	
    val placeDF=h3mapdf
    	.filter(col("harbour").isNull)
    	.drop("harbour")
    	.cache()
    
    return (placeDF,harbourDF)


  }
  
  

  def readALOS(alosDF:DataFrame):DataFrame = {

    val h3df=alosDF.withColumn("H3",geoUDF(col("lat"),col("lon"),lit(10)))
    return h3df
    
  }
  
  
/*combineDF: combine openstreetmap & alos, 
           get the relations: name -> lan,lon
           get flooded, safe df
           get the output orc name | evacuees & sum
*/ 

  def combineDF(df1:DataFrame,df2:DataFrame,riseMeter:Int):(DataFrame,DataFrame)={
  
  /******** Combine osm and alos with h3 value ********/
  //combinedDF - name,place,population,H3,H3Rough,min(elevation)
    val combinedDF_pre = df1
    	.join(df2,Seq("H3"),"inner")
    	
    val combineMinDF=combinedDF_pre
    	.groupBy("name").min("elevation")
    	.withColumnRenamed("min(elevation)","elevation")
 
    val combinedDF=combinedDF_pre
    	.join(combineMinDF,Seq("name","elevation"))
    	.dropDuplicates("name")
   
 
 
  /**********split into flood and safe df ***********/
  
  //floodDF: place,num_evacuees, H3, H3Rough
      val floodDF=combinedDF
      	.filter(col("elevation")<=riseMeter)
      	.drop("elevation","place") //no need to know the type of flooded place any more
      	.withColumnRenamed("population","num_evacuees")
      	.withColumnRenamed("name","place")
      	.withColumnRenamed("H3","floodH3")
      	.withColumn("num_evacuees",col("num_evacuees").cast("int"))
      	.cache()
      		
   
   
   /*safeDF - safe_name,safe_place,safe_population,H3, H3Rough   
    row satisfied:
    - safe_place == city | harbour */
    
    
      val safeDF=combinedDF
      	.filter(col("elevation")>riseMeter)
      	.drop("elevation")
      	.filter(col("place") === "city")  //the destination must be a city
      	.drop("place")
      	.withColumnRenamed("population","safe_population")
      	.withColumnRenamed("name","destination")
      	.withColumnRenamed("H3","safeH3")
      	.cache()

  
  
   
     return (floodDF,safeDF)

  }
  
  def findClosestDest(floodDF:DataFrame,safeDF:DataFrame,harbourDF:DataFrame) {
  
  /******** find the closest city ***********/
 

 //+----------+------------+-----------+----------+---------------+--------+
 //|place     |num_evacuees|destination|dest_place|safe_population|distance| H3Rough floodH3
 //+----------+------------+-----------+----------+---------------+--------+
 //|Bleiswijk |11919       |Delft      |city      |101386         |101     |
 //|Nootdorp  |19160       |Delft      |city      |101386         |35      |

     val floodToSafe=floodDF   //join flood & safe df with H3Rough, calculate the distance between each place and destination
     .join(safeDF,Seq("H3Rough"),"inner")
     .withColumn("city_distance",distanceUDF(col("floodH3"),col("safeH3")))
     .drop("safeH3")
     .cache()



// +-------------+---------------+----------+------------+---------------+-----------+---------------+
// |city_distance|H3Rough        |place     |num_evacuees|floodH3        |destination|safe_population|
// +-------------+---------------+----------+------------+---------------+-----------+---------------+
// |101          |83196bfffffffff|Bleiswijk |11919       |8a196bb2e347fff|Delft      |101386         |
// |28           |83196bfffffffff|Oegstgeest|23608       |8a19694b2417fff|Leiden     |123753         |



     val closestDest=floodToSafe //find the closest city for each flooded place, in "closestDest" each place is distinct
       .join(
       floodToSafe.groupBy("place")
       .min("city_distance")
       .withColumnRenamed("min(city_distance)","city_distance")
       ,Seq("city_distance","place") 
       )
       
     
   

// +-------------+---------------+----------+------------+---------------+-----------+---------------+---------------+---------------+----------------+
// |city_distance|H3Rough        |place     |num_evacuees|floodH3        |destination|safe_population|harbourH3      |H3Rough        |harbour_distance|
// +-------------+---------------+----------+------------+---------------+-----------+---------------+---------------+---------------+----------------+
// |101          |83196bfffffffff|Bleiswijk |11919       |8a196bb2e347fff|Delft      |101386         |8a1fa4926007fff|83196bfffffffff|358             |
// |28           |83196bfffffffff|Oegstgeest|23608       |8a19694b2417fff|Leiden     |123753         |8a1fa4926007fff|83196bfffffffff|539               
     
    
    /******* find the closest harbour *******/
    
     val floodToSafeCH=closestDest  //join place,dest with harbour by H3Rough, calculate the distance between each place and harbour
     .join(harbourDF,closestDest("H3Rough") === harbourDF("H3Rough"),"leftouter") //join by H3Rough
     .withColumn("harbour_distance",distanceUDF(col("floodH3"),col("harbourH3")))
     .drop("H3Rough","floodH3","harbourH3")
     .cache()

    
     val floodMinDF=floodToSafeCH
     	.groupBy("place")
     	.min("harbour_distance")
     	.withColumnRenamed("min(harbour_distance)","harbour_distance") //place is distinct
     	
     val closestCH=floodToSafeCH
     .join(floodMinDF,Seq("harbour_distance","place")) //for each flooded place, find the distance to the nearest harbour
    
     
      
     // seperate into two dataframes
     // near_harbour: places that are closer to a harbour than a safe city
     // near_city: places that are closer to a safe city
     
    //********** divide into 2 DFs ***********
    val near_harbour = closestCH.
     filter(col("harbour_distance") <= col("city_distance")).
     drop("city_distance","harbour_distance").cache()
     
     println("cities closer to a harbour")
     //near_harbour.show(5,false) // close to harbour
     /*
     	+-----+------------+-----------+---------------+
	|place|num_evacuees|destination|safe_population|
	+-----+------------+-----------+---------------+
	|B    |100         |C137       |1000           |
	|C    |100         |C137       |1000           |
	+-----+------------+-----------+---------------+

     */

     
     
     val near_city = closestCH.
     filter(col("harbour_distance") > col("city_distance")).
     drop("harbour_distance","city_distance")
     
     println("cities closer to a safe city")
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
     val change_dest = near_harbour.withColumn("destination",lit("Waterworld")) // change the destination
     val change_popu = change_dest.
     withColumn("num_evacuees",col("num_evacuees")*0.25). // evacuees to the WaterWorld
     withColumn("safe_population", col("safe_population") * 0) // set the population of WaterWorld to 0
     val rest_popu = near_harbour.withColumn("num_evacuees",col("num_evacuees")*0.75)  // evacuees to the nearest city
     val near_harbour_new = rest_popu.union(change_popu).sort("place")	// Combined DF
     
     println("evacuees to harbour and city")
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
     
     val relocate_output = near_harbour_new.union(near_city).
     sort("place")// Combine <near_harbour_new> and <near_city>
     
     println("output => evacuees by place")
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
     
     
     
     println("***************************************")
     println("*********** Saving data ***************")
    // relocate_output.drop("safe_population").write.orc("relocate.orc") // output as .orc file
     println("********** Finished save **************")
     /* change the schema? 
     val schema = StructType(
               Array(
                 StructField("place", StringType),
                 StructField("num_evacuees", LongType)
                 StructField("destination", StringType)
               )
             ) // set the schema of the output data
     val output_12 = spark.createDataFrame(spark.sparkContent.parallelize(relocate_output),schema) //re-create data with the required schema
     output_12.write.orc("relocate_output_12.orc")
     
     val testread = spark.read.format("orc").load("output_12.orc") 
     */
     
     // ********* calculate the total number of evacuees to each destination ********
     val receive_popu = relocate_output.groupBy("destination").
     agg(
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
     
     /********calculate the sum of evacuees********/
    

     	
     val sum_popu = receive_popu
     	.groupBy()
     	.agg(sum("evacuees_received"))
     	.first
     	.get(0)
	
     println("***************************************")
     println("total number of evacuees is " + sum_popu)
     println("***************************************") 
     
     
     // ******* transform the output data into the required format **********
     val receive_output = receive_popu.
     withColumn("new_population",col("old_population") + col("evacuees_received")).
     drop("evacuees_received")
  
     println("output => population change of the destination ")
     //receive_output.show(50,false)
        /*
	+-----------+--------------+--------------+
	|destination|old_population|new_population|
	+-----------+--------------+--------------+
	|Waterworld |0.0           |50.0          |
	|C137       |1000.0        |1450.0        |
	+-----------+--------------+--------------+

     */
     println("***************************************")
     println("*********** Saving data ***************")
     receive_output.write.orc("receive_output_13.orc")
     println("********** Finished save **************")
  }


}

object h3Helper {
  val h3=H3Core.newInstance()
  def toH3func(lat:Double,lon:Double,res:Int):String =
    h3.geoToH3Address(lat,lon,res)
    
  def getH3Distance(origin:String,des:String):Int ={
    if (des != null)  //if no harbour in the hexagon, the distance to harbour will be set to 100000 
                      //(which is definitely bigger than the distance to any city in that hexagon
    	return h3.h3Distance(origin,des)
    else
    	return 100000
    }
  

}


