package lab1

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}

object GDeltRDD {

    val spark = SparkSession
        .builder
        .appName("GDelt")
        .config("spark.master", "local")
        .getOrCreate()
    val sc = spark.sparkContext // If you need SparkContext object


    def formatDate(date: String): String = {
        return "%s-%s-%s".format(date.substring(6,8),date.substring(4,6),date.substring(0,4))
    }

    def main(args: Array[String]) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        
        val spark = SparkSession
        .builder
        .appName("GDeltRDD")
        .config("spark.master", "local")
        .getOrCreate()
        val sc = spark.sparkContext // If you need SparkContext object

        val rawData = sc.textFile("/home/andre/tudelft/supercomputing/lab1/segment/20150218230000.gkg.csv")
        val columns = rawData.map(line => line.split("\t"))
        val fullColumns = columns.filter(list => list.length > 23)
        val noFalsePositives = fullColumns.filter(list => !(list(23) contains "Category"))
        val datedWords = noFalsePositives.map(list => (formatDate(list(1)), list(23).split(";")))
                                         .flatMap(pair => pair._2.map(word => ((pair._1, word.split(",")(0)), 1)))
        val groupedDates = datedWords.reduceByKey((x,y) => x+y)
        val sorted = groupedDates.sortBy(-_._2)

        sorted.take(10).foreach(println)

        spark.stop
  }

}