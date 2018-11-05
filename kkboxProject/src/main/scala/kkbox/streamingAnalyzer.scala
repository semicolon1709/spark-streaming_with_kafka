package kkbox

import java.text.SimpleDateFormat
import kkbox.myCase.kkboxRecordCase
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import kkbox.myDef.defSet.startAndWaitGracefulShutdown


object streamingAnalyzer extends App{

  val conf = new SparkConf().setAppName("kkboxStreamingAnalyzer")
  val ssc = new StreamingContext(conf, Seconds(15))
  ssc.checkpoint("hdfs://master:8020/sparkCheckpoint")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "master:9092, slaver1:9092, slaver2:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kkboxStreamingAnalyzer",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val fromTopics = Array("kkboxINPUT")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferBrokers,
    Subscribe[String, String](fromTopics, kafkaParams)
  )

  //  stream to case class
  val dataToCase = stream.transform(rdd => {
    val songMapBC = mySongMap.getInstance(rdd.sparkContext)
    rdd.flatMap(
      line => {
        val sample = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val s = line.value().split(",")
        try {
          List(kkboxRecordCase(s(0), songMapBC.value.getOrElse(s(1), "none"), s(2), s(3), s(4), sample.parse(s(5))))
        }
        catch {
          case e : Throwable => println("Wrong line format ("+e+"): " + line.value())
            List()
        }
      }
    )
  })

  //find top5 Singers
  val singers = dataToCase.map(kkboxRecord => (kkboxRecord.singer, 1))
    .filter(_._1 != "Various Artists")

  //  mapWithState(top5SongsAllTimes)
  val updateSingerState = (singer:String, hit:Option[Int], state:State[Int]) => {
    var total = hit.getOrElse(0)
    if(state.exists())  total += state.get()
    state.update(total)
    Some((singer, total))
  }
  val top5RecordsState = singers.reduceByKey(_+_)
    .mapWithState(StateSpec.function(updateSingerState).numPartitions(12))
    .stateSnapshots()

  val top5SingersAllTimes = top5RecordsState.transform(_
    .sortBy(_._2, false)
    .zipWithIndex()
    .filter(_._2 < 5)
    .map(ele => ("Top5 Singers(All Times)", ele._1))
    .aggregateByKey(List[(String, Int)]())(
      (x, y) => x :+ y,
      (x, y) => x ++ y
    )
  )

  //  windows(top5SingersPast3Min)
  val top5RecordsWindows = singers.reduceByKeyAndWindow((a1:Int, a2:Int) => a1+a2, Minutes(3))
  val top5SingersPast3Min = top5RecordsWindows.transform(_
    .sortBy(_._2, false)
    .zipWithIndex()
    .filter(_._2 < 5)
    .map(ele => ("Top5 Singers(Past 3 Minutes)", ele._1))
    .aggregateByKey(List[(String, Int)]())(
      (x, y) => x :+ y,
      (x, y) => x ++ y
    )
  )

  val all = top5SingersAllTimes.union(top5SingersPast3Min)

  //  reformat then send to kafka
  all.foreachRDD(rdd =>{
    val dataCollect = rdd.collect()
    dataCollect.foreach(ele =>{
      val produceTo = new myKafkaProducer()
      produceTo.sendToKafkaAsync("kkboxAnalyzer", ele._1, ele._1 + ":" + ele._2.mkString(","))
      println(s"key is ${ele._1}")
    })
  })

  //  async commit to kafka
  stream.foreachRDD(consumerRecord => {
    val offsetRanges = consumerRecord.asInstanceOf[HasOffsetRanges].offsetRanges
    stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  })

  startAndWaitGracefulShutdown(ssc, "/kkboxAnalyzerStop")
}
