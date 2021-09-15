import org.apache.spark.sql.SparkSession
import com.uber.h3core.H3Core
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

object Lab1 {
  //def myFunc: (Column => Double) = { s => s.toDouble }
  
  def main(args: Array[String]) {
    // Create a SparkSession
    val spark = SparkSession
        .builder()
        .appName("Lab 1")
        .config("spark.master", "local")
        .getOrCreate()
   // @transient lazy val h3=H3Core.newInstance()
   // val  lat = 50.9701101;
   // val  lng = 5.7610329;
   // val  res = 10;
   // println("************************************");
   // val str: String=h3.geoToH3Address(lat, lng, res);
   // println(str);
   // println("hehe") 
   // println(args(0));
    //println(h3.h3ToGeo(str));
    val df1=readOpenStreetMap(spark.read.format("orc").load("netherlands-latest.osm.orc"));
   // val df2=readALOS(spark.read.load("parquet/*"),args(0).toInt);
     val df2=readALOS(spark.read.load("parquet/ALPSMLC30_N052E006_DSM.parquet"),args(0).toInt);
    combineDF(df1.select(col("name"),col("place"),col("population"),col("H3")),df2.select(col("H3"),col("elevation")),args(0).toInt);
    // Stop the underlying SparkContext
    spark.stop
  }

  def readOpenStreetMap(df:DataFrame) : DataFrame = {
    val lessdf=df.select(col("id"),col("type"),col("lat"),col("lon"),explode(col("tags")))
      .filter(col("key") ==="name" || col("key") === "place" || col("key") === "population");
    println("hehe2");
    val groupdf=lessdf
      .groupBy("id","type","lat","lon")
      .pivot("key", Seq("name", "place", "population"))
      .agg(first("value"))
    val groupdf2=groupdf.filter(col("type") === "node" && col("population").isNotNull);
    println("hehe3");
    val geoUDF = udf((lat: Double, lon:Double) => h3Helper.toH3func(lat,lon,10))
    println("hehe4");
    val h3mapdf=groupdf2.withColumn("H3",geoUDF(col("lat"),col("lon")));
    return h3mapdf
    // h3mapdf.show(10,false)


  }

  def readALOS(alosDF:DataFrame,riseMeter:Int):DataFrame = {
    val geoUDF = udf((lat: Double, lon:Double) => h3Helper.toH3func(lat,lon,10))
    val h3df=alosDF.withColumn("H3",geoUDF(col("lat"),col("lon")));
    return h3df
    //h3df.show(5,false)
   

  }

  def combineDF(df1:DataFrame,df2:DataFrame,riseMeter:Int){
    val output = df1.join(df2,df1("H3")===df2("H3"),"inner")
      .drop("H3")
      .groupBy("name","place","population")
      .avg("elevation")
      .filter(col("avg(elevation)")<=riseMeter);
    
    output.show(10,false);

  }


}

object h3Helper {
  val h3=H3Core.newInstance()
  def toH3func(lat:Double,lon:Double,res:Int):String =
    h3.geoToH3Address(lat,lon,10)

}
