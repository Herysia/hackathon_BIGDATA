/**
Scala script using sparkSQL used to load and modify INSEE dataset about hotel nights (https://www.insee.fr/fr/statistiques/series/113990189?INDICATEUR=114799559)
- Remove/Rename some columns
- Cross Data table and caracterisation table
- Filter only usefull columns
- Add key-identical columns

Contributors: DAUPHIN Yohan - GIRERD Thomas - LAI-KING Mathieu - VILIN Victor
Date: 11/21/2020
*/
import org.apache.spark.sql.functions._
import org.apache.spark.broadcast._
import org.apache.spark.sql.DataFrame

//Chargement des fichiers d'entrée
val caract = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("/data/hackathon/donnees_groupes_2/climat/caract.csv")
val mensuel = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("/data/hackathon/donnees_groupes_2/climat/valeurs_mensuelles.csv")
/**
* ? Useless ?
//Modification de meteo, on crée une colonne "01 - Ain", au lieu de 2 colonnes 01 (depNum) et Ain (departement)
meteo = meteo.withColumn($"departement", concat($"depNum", lit("-"), $"departement"))
*/

//Remove unwanted rows
val caract_filtered_row = caract.where($"Périodicité" === "Mensuelle" && $"Indicateur".contains("Nombre de nuitées"))

//Remove unwanted columns
val caract_filtered_all = caract_filtered_row.select("idBank", "Zone géographique", "Pays de résidence des touristes", "Indicateur")

//rename columns
val caract_final = caract_filtered_all.withColumnRenamed("Zone géographique", "departement").withColumnRenamed("Pays de résidence des touristes", "residence").withColumnRenamed("Indicateur", "indicateur")

//remove useless columns
val mensuel_filtered = mensuel.drop("Libellé", "Période")

//Croisement des tables caract et mensuel (+ remove useless columns)
val hotels = mensuel_filtered.join(caract_final, Seq("idBank"), "inner").drop("idBank")

//Normalize departement column name
val columnList = for { year <- 2010 until 2020; month <- 1 until 12} yield (year.toString + "-" + "%02d".format(month))
val exprs = for { c <- columnList} yield sum(col(c)).alias(c)

//Function to format departement column
val sqlFormatDepartement = udf((departement:String) => {
    //Si la valeure n'est pas formée: 01 - Ain , ce n'est pas un département mais une région -> on ignore
    //Sinon: on récupère seulement la partie Département
    
    var ret = ""
    val data:Array[String] = departement.split(" - ")
    if(data.size > 1)
        try {
            val dep = data(0).toInt
            ret = data.slice(1,data.size).mkString(" - ")
        } catch {
            //1st value isn't an in
            case _: Throwable => 
        }
    ret
})

//! On remarque qu'il n'y a que les hotels avec une périodicité mensuelle

val hotels_formated = hotels.withColumn("departement", sqlFormatDepartement(col("departement"))).where($"departement" =!= "")
//hotels_formated.select("departement").show()

//Merge identical rows (groupby, agg, sum)
val hotels_compressed = hotels_formated.drop("indicateur").groupBy("departement", "residence").agg(exprs.head, exprs.tail: _*)

//Write result to csv file
hotels_compressed.coalesce(1).write.format("csv").option("header", "true").save("/user/formation46/out/out_hotels")
