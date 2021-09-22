
     
     import spark.implicits._
     // ************* simple-data *************
     val columns = Seq("place","city_distance","harbour_distance","num_evacuees","destination","safe_population")
     val data = Seq(("A",100,200,100,"C137",1000),("B",300,200,100,"C137",1000),("C",400,200,100,"C137",1000),("D",50,200,100,"C137",1000),("E",100,500,100,"C137",1000))
     val rdd = spark.sparkContext.parallelize(data)
     val closestCH = rdd.toDF("place","city_distance","harbour_distance","num_evacuees","destination","safe_population")
	closestCH.printSchema()
	closestCH.show(false)
	
	//***********divide into 2 DFs ***********
    val near_harbour = closestCH
     .filter(col("harbour_distance") <= col("city_distance"))
     .drop("city_distance","harbour_distance")
     near_harbour.show(50,false) // close to harbour
   val   
     
     
     val near_city = closestCH
     .filter(col("harbour_distance") > col("city_distance"))
     .drop("harbour_distance","city_distance")
     
     near_city.show(50,false) // close to city
