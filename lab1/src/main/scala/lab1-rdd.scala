package lab1

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.Timestamp

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.immutable.TreeSet

import org.apache.log4j.{Level, Logger}

object GDeltRDD {


    def formatDate(date: String): String = {
        return "%s-%s-%s".format(date.substring(6,8),date.substring(4,6),date.substring(0,4))
    }

    def createHashMap(wordsStr: String): Map[String,Int] = {
        val wordWithOffsetArr = wordsStr.split(';')

        val words = HashMap[String,Int]().withDefaultValue(0)

        for (entry <- wordWithOffsetArr) {
            val word : String = entry.split(',')(0)

            if(!(word contains "Category"))
                words(word) += 1
        }

        return words
    }

    def mergeHashMaps(map1: Map[String,Int], map2: Map[String,Int]) : Map[String,Int] = {
        
        for((k,v) <- map2) {
            map1(k) += v
        }
        
        return map1
    }

    def retrieveMostCommon(map: Map[String, Int], n: Int): Array[(String,Int)] = {
        var sorted = TreeSet[(String,Int)]()(Ordering.by(-(_: (String,Int))._2))
        for((k,v) <- map) {
            sorted = sorted + ((k,v))
        }
        val list = sorted.toArray
        return list.slice(0,n)
    }

    def main(args: Array[String]) {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        
        val spark = SparkSession
                                .builder
                                .appName("GDeltRDD")
                                .getOrCreate()
        val sc = spark.sparkContext // If you need SparkContext object
        
        val rawData = sc.textFile(sys.env("FILEPATH"))
        val columns = rawData.map(line => line.split("\t"))
        val fullColumns = columns.filter(list => list.length > 23)
        val documentsHashMap = fullColumns.map(list => (formatDate(list(1)), createHashMap(list(23))))
        val groupedDates = documentsHashMap.reduceByKey(mergeHashMaps)
        val sorted = groupedDates.map(x => (x._1, retrieveMostCommon(x._2, 10)))
        val prettyStr = sorted.map(x => "DateResult(" + x._1 + ",List(" + x._2.deep.mkString(",") + "))")
        sorted.coalesce(1).saveAsTextFile(sys.env("OUTPATH"))
        spark.stop
  }

}