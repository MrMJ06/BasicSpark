package es.us.SparkExamples

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession

/**
  * Created by Jose Maria Luna.
  */
object MainBabyRDD {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("RDD Main")
      .master("local[*]")
      .getOrCreate()


    //TODO: Carga el fichero "baby_names.csv"
    //val dataset = spark.sparkContext.textFile("resources/baby_names.csv")

    val dataframe = spark.read.csv("resources/baby_names.csv").toDF("Year","FirstName","County","Sex","Count")

    //TODO: Saca por pantalla el número de filas que tiene
    println("OUTPUT: "+ dataframe.count())

    //TODO: Saca por pantalla las 15 primeras filas
    dataframe.take(15).foreach(println)

    //TODO: ¿Cuántos años abarca la base de datos?
    dataframe.createOrReplaceGlobalTempView("babyNames")
    spark.sql("select max(year) from global_temp.babyNames").show()

    //TODO: ¿Cuántos nombres hay?
    val names = dataframe.select("FirstName").distinct().count()

    println("OUTPUT: NAMES-> "+names);

    //TODO: ¿Hay más nombres de niños o de niñas?
    val gvb = spark.sql("select first(Sex) from global_temp.babyNames group by FirstName").filter(s=>s.toString().contains("F")).count()

    if((names/2)>gvb)
      println("There are more boy names->"+ (names-gvb))
    else
      println("There are more girl names->"+ gvb)

  }

}
