
     
     import spark.implicits._
     import org.apache.spark.sql.functions._
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

     
     
     val near_city = closestCH.
     filter(col("harbour_distance") > col("city_distance")).
     drop("harbour_distance","city_distance")
     
     near_city.show(5,false) // close to city
     

     // ********* operation on <near_harbour> DF **********
     val change_dest = near_harbour.withColumn("destination",lit("Waterworld")) // change the destination
     val change_popu = change_dest.
     withColumn("num_evacuees",col("num_evacuees")*0.25). // evacuees to the WaterWorld
     withColumn("safe_population", col("safe_population") * 0) // set the population of WaterWorld to 0
     val rest_popu = near_harbour.withColumn("num_evacuees",col("num_evacuees")*0.75)  // evacuees to the nearest city
     val near_harbour_new = rest_popu.union(change_popu).sort("place")	// Combined DF
     
     near_harbour_new.show(50,false) // evacuees to harbour and city
     
     val relocate_output = near_harbour_new.union(near_city).
     drop("safe_population").
     sort("place")// Combine <near_harbour_new> and <near_city>
     
     relocate_output.show(50,false)
     
     relocate_output.write.orc("relocate.orc") // output as .orc file
     
     
          
