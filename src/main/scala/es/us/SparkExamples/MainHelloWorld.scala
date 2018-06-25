package es.us.SparkExamples

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession

/**
  * Created by Jose Maria Luna.
  */
object MainHelloWorld {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("Hello World Main")
      .master("local[*]")
      .getOrCreate()


    println("Hello World")

    //TODO: Crea una variable mutable y otra inmutable
    var mutable: String = "Soy un digimon"
    val inmutable: String = "Soy un pokemon"

    //TODO: Modifica el valor de la variable mutable y de la inmutable
    mutable = "No soy un digimon"
    //inmutable = "No soy un pokemon"

    //TODO: Crea una variable de tipo String y separe sus caracteres por espacios
    val myVar = "Hello"
    val stringSeparado = myVar.split(" ")
    val rddSeparada = spark.sparkContext.parallelize(stringSeparado)

    for (j <- rddSeparada) {
      for (i <- j) {
        print(i + " ")

      }
    }
    println("")
    //TODO: Crea un array
    val myArray = Array(1,2,3,4,"JEJE")

    //TODO: Pasa el array a String e imprimelo por pantalla
    println(myArray.mkString("-"))

    //TODO: Crea una función que sume 1.5 al valor introducido
    def sumNum(num : Double): Double={
      num+1.5
    }

    //TODO: Llama a la función y prueba a pasarle los siguientes parámetros: un entero, un double y un string
    println(sumNum(1))
    println(sumNum(1.5))
    //print(sumNum("1"))


  }

}
