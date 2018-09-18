package example

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._
import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext._

//dataset implementation
object GDelt {
  case class GDeltData ( 
    gkgRecordId: String, 
    date: Timestamp,
    allNames: String
  )

  //in order to deal with strings and without hours :)
  def formatDate(input: Timestamp) : String = {
    val newFormat = new SimpleDateFormat("yyyy-MM-dd")
    return newFormat.format(input.getTime())
  }


  def myprint(input: Row) = {
    val x = input.get(1).asInstanceOf[Seq[(String,Int)]].take(10)
    printf("%s : (%s) \n", input.get(0), x)
  }


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("GDelt")
      .config("spark.master", "local")
      .getOrCreate()
    
    import spark.implicits._
    
    val schema =
      StructType(
        Array(
          StructField("GKGRECORDID", StringType, nullable=true),
          StructField("DATE", TimestampType, nullable=true),
          StructField("SourceCollectionIdentifier", LongType, nullable=true),
          StructField("SourceCommonName", StringType, nullable=true),
          StructField("DocumentIdentifier", StringType, nullable=true),
          StructField("Counts", StringType, nullable=true),
          StructField("V2Counts", StringType, nullable=true),
          StructField("Themes", StringType, nullable=true),
          StructField("V2Themes", StringType, nullable=true),
          StructField("Locations", StringType, nullable=true),
          StructField("V2Locations", StringType, nullable=true),
          StructField("Persons", StringType, nullable=true),
          StructField("V2Persons", StringType, nullable=true),
          StructField("Organizations", StringType, nullable=true),
          StructField("V2Organizations", StringType, nullable=true),
          StructField("V2Tone", StringType, nullable=true),
          StructField("Dates", StringType, nullable=true),
          StructField("GCAM", StringType, nullable=true),
          StructField("SharingImage", StringType, nullable=true),
          StructField("RelatedImages", StringType, nullable=true),
          StructField("SocialImageEmbeds", StringType, nullable=true),
          StructField("SocialVideoEmbeds", StringType, nullable=true),
          StructField("Quotations", StringType, nullable=true),
          StructField("AllNames", StringType, nullable=true),
          StructField("Amounts", StringType, nullable=true),
          StructField("TranslationInfo", StringType, nullable=true),
          StructField("Extras", StringType, nullable=true)
        )
      )

    //val sc = spark.sparkContext // If you need SparkContext object

    val ds = spark.read 
                  .schema(schema) 
                  .option("timestampFormat", "MMddyyhhmm")
                  .option("delimiter", "\t")
                  .csv("/home/ines/Documents/SBD/supercomputing-for-big-data/lab1/segment/*.gkg.csv")  //TODO remove
                  .as[GDeltData]

    //clean up + create structure ((date,name), count)
    val getPairs =  ds.filter(x => x.allNames != null)
                        .map(x => (formatDate(x.date), x.allNames.split(";")))
                        .flatMap(x => (x._2.map( y => ((x._1, y.split(",")(0)),1))))  
                        .filter(x => !(x._1._2 contains "ParentCategory"))
    
    //count
    val getCount = getPairs.groupByKey(_._1)
                        .reduceGroups((a, b) => (a._1, a._2 + b._2))
                        .map(_._2).as("theme")
                        .sort($"theme._2".desc)
                        .map(x => (x._1._1, (x._1._2, x._2)))
              
    val new_ds = getCount.toDF("date","values")
    val sort = new_ds.groupBy("date")
                    .agg(collect_list($"values"))
    
    sort.collect.foreach(myprint)

    spark.stop
  }
}

