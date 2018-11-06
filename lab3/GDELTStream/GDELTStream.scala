package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.{Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import org.apache.kafka.streams.state.{Stores,StoreBuilder,KeyValueStore,KeyValueIterator}

import scala.collection.JavaConversions._

object GDELTStream extends App {
  import Serdes._

  def getWordList(wordsStr: String): Array[String] = {
    val wordWithOffsetArr = wordsStr.split(';')

    var words = Array[String]()

    for (entry <- wordWithOffsetArr) {
      val word : String = entry.split(',')(0)
      if(!(word contains "Category") && word != "")
        words = words :+ word
    }
    return words
  }

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

    // KeyValueStore<String, Long> countStore = countStoreSupplier.build();

  val builder: StreamsBuilder = new StreamsBuilder
  val timestampStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("timestampStore"),
    Serdes.String,
    Serdes.Long
  )
  builder.addStateStore(timestampStoreBuilder)
  val histogramStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("histogramStore"),
    Serdes.String,
    Serdes.Long
  )
  builder.addStateStore(histogramStoreBuilder)

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally, 
  // write the result to a new topic called gdelt-histogram. 
  val records: KStream[String, String] = builder.stream[String, String]("gdelt")

  val wordList = records.map((x,line) => (x,line.split("\t")))
                        .filter((x,list) => list.length > 23)
                        .flatMapValues(list => getWordList(list(23)))
                        // .flatMapValues(list => list)

  
  wordList.transform(new HistogramTransformer(), "histogramStore", "timestampStore").to("gdelt-histogram")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()



  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

  System.in.read()
  System.exit(0)
}

// This transformer should count the number of times a name occurs 
// during the last hour. This means it needs to be able to 
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _
  var histogramStore: KeyValueStore[String, Long] = _
  var timestampStore: KeyValueStore[String, Long] = _

  val ONE_HOUR = 60*60


  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
    this.histogramStore = context.getStateStore("histogramStore").asInstanceOf[KeyValueStore[String, Long]]
    this.timestampStore = context.getStateStore("timestampStore").asInstanceOf[KeyValueStore[String, Long]]

    this.context.schedule(5, PunctuationType.WALL_CLOCK_TIME, (timestamp: Long) => {
      val currentTimestamp = timestamp/1000
      val iter: KeyValueIterator[String,Long] = this.timestampStore.all();
      var exit = false
      
      if(iter.hasNext()) {
        val entry = iter.next();

        val split = entry.key.split('-')
        val keyTimestamp: Long = split(0).toLong
        val keyWord: String = split(1)

        if(currentTimestamp - keyTimestamp  < ONE_HOUR){
          exit = true
        }
        else {
          timestampStore.delete(entry.key)
          var currentCount = this.histogramStore.get(keyWord)
          var newCount = currentCount - entry.value
          if(newCount < 0){ // shouldn't happen - removing while removed concurrency issues
            newCount = 0
          }
          this.histogramStore.put(keyWord,currentCount-entry.value)
          context.forward(keyWord,currentCount-entry.value)
        }
          iter.close();
      }
    })
  }

  // Should return the current count of the name during the _last_ hour
  def transform(key: String, name: String): (String, Long) = {
    val timestampSecs = context.timestamp()/1000

    var occurrencesInSecond = this.timestampStore.get(timestampSecs.toString+"-"+name)
    if(!occurrencesInSecond.equals(null))
      occurrencesInSecond += 1L
    else
      occurrencesInSecond = 1L

    var totalOccurences = this.histogramStore.get(name)
    if(!totalOccurences.equals(null))
      totalOccurences += 1L
    else
      totalOccurences = 1L
    
    this.histogramStore.put(name, totalOccurences)
    this.timestampStore.put(timestampSecs.toString+"-"+name, occurrencesInSecond)

    (name, totalOccurences)
  }

  // Close any resources if any
  def close() {
  }
}
