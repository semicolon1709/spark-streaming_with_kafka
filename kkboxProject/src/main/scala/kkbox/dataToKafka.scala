package kkbox

import java.util.Calendar
import java.text.SimpleDateFormat
import scala.io.Source
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

object dataToKafka extends App{

  val threadNum = 15
  val sample = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val topic = "kkboxINPUT"
  val kkboxRecords = Source.fromFile("/home/yunhan/kkbox/kkbox.csv").getLines().toArray
  val producer = new myKafkaProducer()
  concurrent(5 to 1999999, threadNum)

  def concurrent(dataIndexes: Seq[Int], threadNum: Int) = {
    val loopPar = dataIndexes.par // concurrent集合
    loopPar.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(threadNum))
    loopPar.foreach(worker(_))
  }

  def worker(dataIndex: Int): Unit= {
    val data = kkboxRecords(dataIndex).split(",").drop(1)
    val key = data(0)
    producer.props.put("client.id", data(0))
    val message = (data :+ sample.format(Calendar.getInstance().getTime())).mkString(",")
    Thread.sleep(3)
    producer.sendToKafkaAsync(topic, key, message)
  }
}