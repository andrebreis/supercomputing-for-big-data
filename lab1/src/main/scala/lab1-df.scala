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
object GDelt_df {
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


  //print results
  def myprint(input: Row) = {
    val x = input.get(1).asInstanceOf[Seq[(String,Int)]].take(10)
    printf("%s : (%s) \n", input.get(0), x)
  }


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("GDelt_df")
      .config("spark.master", "local")
      .getOrCreate()
    
    import spark.implicits._
    
    //first schema that reads the data of csv
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

    val ds = spark.read 
                  .schema(schema) 
                  .option("timestampFormat", "yyyyMMddHHmmss")
                  .option("delimiter", "\t")
                  .csv("/home/ines/Documents/SBD/supercomputing-for-big-data/lab1/segment/*.gkg.csv")  //TODO remove
                  .as[GDeltData]

    val t1 = ds.filter(x => x.allNames != null)                             //remove lines without articles
                .map(x => (formatDate(x.date), x.allNames.split(";")))      //separate articles
                .flatMap(x => (x._2.map( y => (x._1, y.split(",")(0), 1)))) //create a row (date, article, count)  
                .as("gdelt")
                .filter(x => !(x._2 contains "ParentCategory"))          //remove this parent categories

    //sum same articles from the same day ans sort in descending order
    val t2 = t1.groupBy($"gdelt._1", $"gdelt._2").sum()
                .sort($"sum(_3)".desc)
               
    //create structure { date , List(article, count) }
    val t3 =  t2.withColumn("pairs", struct($"gdelt._2", $"sum(_3)"))
                .groupBy("gdelt._1")
                .agg(collect_list("pairs") as "pairs")
                //.sort($"gdelt._1")
    
    //top 10 - does not work
    //val t4 = t3.map(row => (row.getAs[String](0), row.getAs[Seq[Tuple2[String,Int]]](1)))
    //t4.show()

    //print top 10
    t3.collect.foreach(myprint)


    spark.stop
  }
}