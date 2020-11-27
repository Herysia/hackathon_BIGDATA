/**
Scala script using sparkSQL used to cross data generated from "inseeTrousistsFormat.scala" and "noaaFilter.scala"

Contributors: DAUPHIN Yohan - GIRERD Thomas - LAI-KING Mathieu - VILIN Victor
Date: 11/21/2020
*/

import org.apache.spark.sql.functions._
import org.apache.spark.broadcast._
import org.apache.spark.sql.DataFrame

//Chargement des fichiers d'entrée
var meteo = spark.read.format("csv").option("header", "true").load("/user/formation46/out/meteo.csv")
val hotels = spark.read.format("csv").option("header", "true").load("/user/formation46/out/hotels.csv")

/** 
// ? Useless 

val columnList = for { year <- 2010 until 2020; month <- 1 until 12} yield (year.toString + "-" + "%02d".format(month))
var exprs = for { c <- columnList} yield sum(col(c)).alias(c)


//Merge identical rows (groupby, agg, sum)
val hotels_compressed_indicateur = hotels.drop("residence").groupBy("departement", "indicateur").agg(exprs.head, exprs.tail: _*)
val hotels_compressed_residence = hotels.drop("indicateur").groupBy("departement", "residence").agg(exprs.head, exprs.tail: _*)
*/


// https://stackoverflow.com/questions/55084996/why-dataframe-cannot-be-accessed-inside-udf-apache-spark-scala
// We create a broadcast to be able to access it from sql (when applying udf)
val bc_hotels = spark.sparkContext.broadcast(hotels.collect.map(r => Map(hotels.columns.zip(r.toSeq):_*)))


///////////////////////////////////////////////////
//Ajout du nombre de nuit mensuel par département//
///////////////////////////////////////////////////

//Function to count the number of nights for a give Department, Year, Month and residence
val nightCount: ((String, String, String, String) => String) = (departement:String, year:String, month:String, residence:String) => {

    var nbNuits = 0.0

    bc_hotels.value.foreach((row) => {
        if(row("departement").toString.contains(departement)) {
            try {
                if(row("residence").toString.equals(residence)) {
                    val date = year.toString + "-" + month.toString
                    nbNuits += row(date).toString.toFloat
                }
            } catch {
                case e: Exception => e.printStackTrace()
                //case _: Throwable => 
            }
        }
    })

    nbNuits.toString
}
//nightCount("Rhône", "2018", "01", "France")

val sqlNightCount_fr = udf((departement:String, year:String, month:String) => nightCount(departement, year, month, "France"))
val sqlNightCount_foreign = udf((departement:String, year:String, month:String) => nightCount(departement, year, month, "Étranger"))
val sqlNightCount_all = udf((departement:String, year:String, month:String) => nightCount(departement, year, month, "Ensemble"))

//Add night count columns -> one for each 3 [French, Foreigners, All])
val resultat = meteo.withColumn("NIGHTS_FR", sqlNightCount_fr(col("DEPARTEMENT"), col("YEAR"), col("MO"))).withColumn("NIGHTS_FOREIGN", sqlNightCount_foreign(col("DEPARTEMENT"), col("YEAR"), col("MO"))).withColumn("NIGHTS_ALL", sqlNightCount_all(col("DEPARTEMENT"), col("YEAR"), col("MO")))


//Write results to csv
resultat.coalesce(1).write.format("csv").option("header", "true").save("/user/formation46/out/out_result")
