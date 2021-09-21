import org.apache.spark.sql.SparkSession
import com.uber.h3core.H3Core
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import scala.sys.process._ 

object Lab1 {

  def main(args: Array[String]) {
    // Create a SparkSession
    val spark = SparkSession
        .builder()
        .appName("Lab 1")
        .config("spark.master", "local")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  //  val (df1,harbourDF)=readOpenStreetMap(spark.read.format("orc").load("zuid-holland-latest.osm.orc"));  //partial osm dataset - corresponds to N052E005
    val (df1,harbourDF)=readOpenStreetMap(spark.read.format("orc").load("netherlands-latest.osm.orc")); //complete osm dataset
    val df2=readALOS(spark.read.load("parquet/*"));    //complete alos dataset 
    // val df2=readALOS(spark.read.load("parquet/ALPSMLC30_N052E005_DSM.parquet")); //partial alos dataset
     val (floodDF, safeDF)=combineDF(df1.select(col("name"),col("population"),col("H3"),col("place"),col("H3Rough")),df2.select(col("H3"),col("elevation")),args(0).toInt);
    // Stop the underlying SparkContext
    findClosestDest(floodDF,safeDF,harbourDF)
    spark.stop
  }

  def readOpenStreetMap(df:DataFrame) : (DataFrame,DataFrame) = {
    val lessdf=df.select(col("id"),col("type"),col("lat"),col("lon"),explode(col("tags")))
      .filter(col("key") ==="name" || col("key") === "place" || col("key") === "population" || col("key") === "harbour");
    println("hehe2");
    val groupdf=lessdf
      .groupBy("id","type","lat","lon")
      .pivot("key", Seq("name", "place", "population","harbour"))
      .agg(first("value"))
    //groupdf.printSchema()
    //
    val groupdf2=groupdf
     .filter(col("type") === "node")
     .filter((col("place").isNotNull && col("population").isNotNull && 
    (col("place") ==="city" || col("place") ==="town" ||col("place")==="village" || col("place") ==="halmet" )) || col("harbour") === "yes" )
    
    
    //groupdf2.write.save("alldata.parquet")
   // groupdf2.filter(col("harbour") === "yes").show(10,false)
   // sys.exit(0)
    println("hehe3");
    val geoUDF = udf((lat: Double, lon:Double, res: Int) => h3Helper.toH3func(lat,lon,res))
    println("hehe4");
    val h3mapdf=groupdf2.withColumn("H3",geoUDF(col("lat"),col("lon"),lit(10)))
    .withColumn("H3Rough",geoUDF(col("lat"),col("lon"),lit(3)));
    
    val harbourDF=h3mapdf.filter(col("harbour") === "yes" ).select(col("H3").as("harbourH3"),col("H3Rough"))
    val placeDF=h3mapdf.filter(col("harbour").isNull).drop("harbour")
    //placeDF.show(false)
    return (placeDF,harbourDF)


  }

  def readALOS(alosDF:DataFrame):DataFrame = {
    val geoUDF = udf((lat: Double, lon:Double, res: Int) => h3Helper.toH3func(lat,lon,res))
    val h3df=alosDF.withColumn("H3",geoUDF(col("lat"),col("lon"),lit(10)))
    return h3df
    //h3df.show(5,false)
   

  }
//combineDF: combine openstreetmap & alos, 
//           get the relations: name -> lan,lon
//           get flooded, safe df
//           get the output orc name | evacuees & sum
  def combineDF(df1:DataFrame,df2:DataFrame,riseMeter:Int):(DataFrame,DataFrame)={
  
  //combinedDF - name,place,population,H3,H3Rough,min(elevation)
    val combinedDF_pre = df1.join(df2,Seq("H3"),"inner")
    val combine2=combinedDF_pre.groupBy("name").min("elevation").withColumnRenamed("min(elevation)","elevation")
      
    val combinedDF=combinedDF_pre.join(combine2,Seq("name","elevation")).dropDuplicates("name")
    print("*******************************************************************************************************")
  //  print("the original rows: "+combinedDF.count()+"after dropDuplicate: "+combinedDF.dropDuplicates("name").count()+"after drop name elevation"+combinedDF.dropDuplicates("name","elevation").count())
   //combinedDF.show(100,false)   
  //floodDF: place,num_evacuees, H3, H3Rough
      val floodDF=combinedDF
      	.filter(col("elevation")<=riseMeter)
      	.drop("elevation","place") //no need to know the type of flooded place any more
      	.withColumnRenamed("population","num_evacuees")
      	.withColumnRenamed("name","place")
      	.withColumnRenamed("H3","floodH3")
      	.withColumn("num_evacuees",col("num_evacuees").cast("int"))
      	
    //  val output = floodDF.drop("H3","H3Rough")
   //    output.show(5)
   //floodDF.show(10,false)
   floodDF.printSchema()   	
   //safeDF - safe_name,safe_place,safe_population,H3, H3Rough   
   // row satisfied:
   // - safe_place == city | harbour
      val safeDF=combinedDF
      	.filter(col("elevation")>riseMeter)
      	.drop("elevation")
      	.filter(col("place") === "city")  //the destination must be a city
      	.drop("place")
      	.withColumnRenamed("population","safe_population")
      	.withColumnRenamed("name","destination")
      	.withColumnRenamed("H3","safeH3")

     //   harbourDF.show(10,false)	
    //safeDF.show(10,false)  
      
    /********calculate the sum of evacuees********/
    //output.write.parquet("alldata.parquet")
     val sum = floodDF.groupBy().sum("num_evacuees").first.get(0)
     println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
     println("========================================");
     println("total number of evacuees is " + sum);
   //  output.show(10,false);
     return (floodDF,safeDF)

  }
  
  def findClosestDest(floodDF:DataFrame,safeDF:DataFrame,harbourDF:DataFrame) {
     val distanceUDF = udf((origin:String,des:String) => h3Helper.getH3Distance(origin,des))	
     
     
 //+----------+------------+-----------+----------+---------------+--------+
 //|place     |num_evacuees|destination|dest_place|safe_population|distance| H3Rough floodH3
 //+----------+------------+-----------+----------+---------------+--------+
 //|Bleiswijk |11919       |Delft      |city      |101386         |101     |
 //|Nootdorp  |19160       |Delft      |city      |101386         |35      |

     val floodToSafe=floodDF   //join flood & safe df with H3Rough, calculate the distance between each place and destination
     .join(safeDF,Seq("H3Rough"),"inner")
     .withColumn("city_distance",distanceUDF(col("floodH3"),col("safeH3")))
     .drop("safeH3")



// +-------------+---------------+----------+------------+---------------+-----------+---------------+
// |city_distance|H3Rough        |place     |num_evacuees|floodH3        |destination|safe_population|
// +-------------+---------------+----------+------------+---------------+-----------+---------------+
// |101          |83196bfffffffff|Bleiswijk |11919       |8a196bb2e347fff|Delft      |101386         |
// |28           |83196bfffffffff|Oegstgeest|23608       |8a19694b2417fff|Leiden     |123753         |



     val closestDest=floodToSafe //find the closest city for each flooded place, in "closestDest" each place is distinct
       .join(floodToSafe.groupBy("place").min("city_distance").withColumnRenamed("min(city_distance)","city_distance"),Seq("city_distance","place")) //join fangfa
       
     
   //  closestDest.show(100,false)

// +-------------+---------------+----------+------------+---------------+-----------+---------------+---------------+---------------+----------------+
// |city_distance|H3Rough        |place     |num_evacuees|floodH3        |destination|safe_population|harbourH3      |H3Rough        |harbour_distance|
// +-------------+---------------+----------+------------+---------------+-----------+---------------+---------------+---------------+----------------+
// |101          |83196bfffffffff|Bleiswijk |11919       |8a196bb2e347fff|Delft      |101386         |8a1fa4926007fff|83196bfffffffff|358             |
// |28           |83196bfffffffff|Oegstgeest|23608       |8a19694b2417fff|Leiden     |123753         |8a1fa4926007fff|83196bfffffffff|539               
     
     val floodToSafeCH=closestDest  //join place,dest with harbour by H3Rough, calculate the distance between each place and harbour
     .join(harbourDF,closestDest("H3Rough") === harbourDF("H3Rough"),"leftouter") //join by H3Rough
     .withColumn("harbour_distance",distanceUDF(col("floodH3"),col("harbourH3")))
     .drop("H3Rough","floodH3","harbourH3")

     
   //  floodToSafeCH.show(100,false) ok
     
     val flood2=floodToSafeCH.groupBy("place").min("harbour_distance").withColumnRenamed("min(harbour_distance)","harbour_distance") //place is distinct
    // flood2.show(100,false) //no duplicate
     val closestCH=floodToSafeCH
     .join(flood2,Seq("harbour_distance","place")) //for each flooded place, find the distance to the nearest harbour
    
      closestCH.show(100,false)
     

     
  
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


