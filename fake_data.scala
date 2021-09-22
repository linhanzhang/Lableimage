
     
     import spark.implicits._
     import org.apache.spark.sql.functions._
     import org.apache.spark.sql.types._

   

     // ************* simple-data *************
     //val columns = Seq("place","city_distance","harbour_distance","num_evacuees","destination","safe_population")
     val data = Seq(("A",100,200,100,"C137",1000),("B",300,200,100,"C137",1000),("C",400,200,100,"C137",1000),("D",50,200,100,"C137",1000),("E",100,500,100,"C137",1000))
     val rdd = spark.sparkContext.parallelize(data)
     val closestCH = rdd.toDF("place","city_distance","harbour_distance","num_evacuees","destination","safe_population")
	closestCH.printSchema()
	closestCH.show(5,false)
	
	//********** divide into 2 DFs ***********
    val near_harbour = closestCH.
     filter(col("harbour_distance") <= col("city_distance")).
     drop("city_distance","harbour_distance")
     
     
     near_harbour.show(5,false) // close to harbour
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
     
     near_city.show(5,false) // close to city
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
     
     near_harbour_new.show(50,false) // evacuees to harbour and city
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
     
     relocate_output.show(50,false)
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
     
     //relocate_output.drop("safe_population").write.orc("relocate.orc") // output as .orc file
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
     	)
     /*
     	+-----------+-----------------+--------------+                                  
	|destination|evacuees_received|old_population|
	+-----------+-----------------+--------------+
	|Waterworld |50.0             |0.0           |
	|C137       |450.0            |1000.0        |
	+-----------+-----------------+--------------+
     */
     
     // ******* transform the output data into the required format **********
     val receive_output = receive_popu.
     withColumn("new_population",col("old_population") + col("evacuees_received")).
     drop("evacuees_received")
  
     receive_output.show(50,false)
        /*
	+-----------+--------------+--------------+
	|destination|old_population|new_population|
	+-----------+--------------+--------------+
	|Waterworld |0.0           |50.0          |
	|C137       |1000.0        |1450.0        |
	+-----------+--------------+--------------+

     */
     receive_output.write.orc("receive_output_13.orc")
          
