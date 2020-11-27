/**
Scala script using sparkSQL used to load and modify the dataset NOAA (https://console.cloud.google.com/bigquery?project=test-nby-161612&p=bigquery-public-data&d=noaa_gsod&page=dataset)
- Extract French data only
- Add columns containing name & number of sensor department
- Group by Department/Year/Month

Contributors: DAUPHIN Yohan - GIRERD Thomas - LAI-KING Mathieu - VILIN Victor
Date: 11/21/2020
*/

import org.apache.spark.sql.functions._
import org.apache.spark.broadcast._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column

//Load input files
val coords = spark.read.format("csv").option("header", "true").load("/user/formation46/out/coords_formatted.csv")
val noaa = spark.read.format("csv").option("header", "true").load((2010 until 2020).map( (i) => {"/data/hackathon/noaa/" + i.toString + "/*"}): _*)


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

//Function to get the closest departement name given coordinates (latitude, longitude)
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


//Function to add a column from coords table (matching departement name)
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

//Function to convert °F to °C
val FtoC: String => String = (f:String) => {((f.toFloat - 32) * 5 / 9).toString}



val sqlClosestDepartment = udf(closestDepartment)
val sqlDepNumCoordsColumn = udf((dep:String) => getCoordsColumn(dep, "Nº"))
val sqlFtoC = udf(FtoC)


//Filter only French Stations
val noaa_FR = noaa.filter(noaa("NAME").contains(", FR"))

//Split Date column into YEAR and MO (month) columns
val year_mo = Seq("YEAR", "MO")
val noaa_FR_date = noaa_FR.withColumn("_tmp", split($"DATE", "-")).select(
    col("*") +: (0 until 2).map(i => col("_tmp").getItem(i).as(year_mo(i))): _*
).drop("_TMP")


//Seq("TEMP", "DEWP", "STP", "VISIB", "WDSP", "MXSPD", "GUST", "MAX", "MIN", "PRCP", "SNDP")
val meanColumnList: Seq[String] = Seq("TEMP", "DEWP", "SLP", "VISIB", "WDSP", "SNDP")
val sumColumnList: Seq[String] = Seq("PRCP")
val maxColumnList: Seq[String] = Seq("MAX", "GUST", "MXSPD")
val minCloumnList: Seq[String] = Seq("MIN")
var exprs = for { c <- meanColumnList} yield mean(col(c)).alias(c)
exprs = exprs ++ (for { c <- sumColumnList} yield sum(col(c)).alias(c))
exprs = exprs ++ (for { c <- maxColumnList} yield max(col(c)).alias(c))
exprs = exprs ++ (for { c <- minCloumnList} yield min(col(c)).alias(c))

//Regroup every days of the same Month and Year into 1 row (mean, sum, ..)
val noaa_FR_grouped = noaa_FR_date.groupBy("STATION", "LATITUDE", "LONGITUDE", "YEAR", "MO").agg(exprs.head, exprs.tail: _*)


//Add column DEPARTEMENT (departement name) & DEPNUM
val noaa_dep = noaa_FR_grouped.withColumn("DEPARTEMENT", sqlClosestDepartment(col("LATITUDE"), col("LONGITUDE"))).withColumn("DEPNUM", sqlDepNumCoordsColumn(col("DEPARTEMENT")))

//Regroup every identical departement
val noaa_dep_grouped = noaa_dep.groupBy("DEPARTEMENT", "DEPNUM", "YEAR", "MO").agg(exprs.head, exprs.tail: _*).withColumn("TEMP", sqlFtoC(col("TEMP"))).withColumn("DEWP", sqlFtoC(col("DEWP"))).withColumn("MIN", sqlFtoC(col("MIN"))).withColumn("MAX", sqlFtoC(col("MAX")))

//Write result to csv file
noaa_dep_grouped.coalesce(1).write.format("csv").option("header", "true").save("/user/formation46/out/out_meteo_all_departement")

