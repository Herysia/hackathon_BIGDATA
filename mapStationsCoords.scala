/**
Scala script using sparkSQL used to load and modify the sensor table of the dataset NOAA (https://console.cloud.google.com/bigquery?project=test-nby-161612&p=bigquery-public-data&d=noaa_gsod&page=dataset)
- Extract French data only
- Add columns containing name & number of sensor department
- Group by Department/Year/Month

Contributors: DAUPHIN Yohan - GIRERD Thomas - LAI-KING Mathieu - VILIN Victor
Date: 11/21/2020
*/
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast._
import org.apache.spark.sql.DataFrame

val coords = spark.read.format("csv").option("header", "true").load("/user/formation46/out/coords_formatted.csv")
val stations = spark.read.format("csv").option("header", "true").load("/data/hackathon/noaa/stations.csv")

// https://stackoverflow.com/questions/55084996/why-dataframe-cannot-be-accessed-inside-udf-apache-spark-scala
// We create a broadcast to be able to access it from sql (when applying udf)
val bc_coords = spark.sparkContext.broadcast(coords.collect.map(r => Map(coords.columns.zip(r.toSeq):_*)))

//Average radius of earth in meters
val AVERAGE_RADIUS_OF_EARTH = 6356752

//Function to calculate the distance between 2 coordinates (latitude/longitude) in meters
def calculateDistance(lat1:Float, lon1:Float, lat2:Float, lon2:Float): Float = {
    val latDistance = Math.toRadians(lat2-lat1)
    val lonDistance = Math.toRadians(lon2-lon1)
    val sinLat = Math.sin(latDistance / 2)
    val sinLon = Math.sin(lonDistance / 2)
    val a = sinLat * sinLat + (Math.cos(Math.toRadians(lat1))) * Math.cos(Math.toRadians(lat2)) * sinLon * sinLon
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH * c).toFloat
}

//extract FR stations
val stations_FR = stations.filter(stations("country") === "FR")

//define closest department calculator
val closestDepartment: ((Float, Float) => String) = (lat:Float, lon:Float) => {
    var closestDist = 0.0
    var closestDep = ""
    //closestDep = bc_coords.value(0)("Département")
    bc_coords.value.foreach((row) => {

        val dep = row("Département").toString
        val lat2 = row("Lat").toString.toFloat
        val lon2 = row("Long").toString.toFloat
        val dist = calculateDistance(lat, lon, lat2, lon2)
        if(closestDep.equals("") || dist < closestDist) {
            closestDist = dist
            closestDep = dep
        }
    })
    closestDep
}


//Add column from department
val getCoordsColumn: ((String, String) => String) = (department:String, c:String) => {
    var ret = ""
    //closestDep = bc_coords.value(0)("Département")
    bc_coords.value.foreach((row) => {
        if(row("Département").toString.equals(department)) {
            ret = row(c).toString
            //Todo break, dunno how to do it in scala
        }
    })
    
    ret
}

closestDepartment(1,1)
val sqlClosestDepartment = udf(closestDepartment)
val sqlDepNumCoordsColumn = udf((dep:String) => getCoordsColumn(dep, "Nº"))

val latlongDist = stations_FR.withColumn("departement", sqlClosestDepartment(col("lat"), col("lon"))).withColumn("depNum", sqlDepNumCoordsColumn(col("departement")))

/*
latlongDist.head()
latlongDist.collect().foreach { row => 
    println(row.mkString(","))
}
*/
latlongDist.coalesce(1).write.format("csv").option("header", "true").save("/user/formation46/out/out_stations")
