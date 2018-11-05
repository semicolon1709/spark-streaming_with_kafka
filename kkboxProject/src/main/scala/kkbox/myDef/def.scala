package kkbox.myDef

import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

object defSet {
  def startAndWaitGracefulShutdown(ssc: StreamingContext, HDFSStopPath:String) = {
    var stopFlag = false
    var isStopped = false
    val checkIntervalMillis = 10000
    ssc.start()
    while (!isStopped) {
      val hdfs = FileSystem.get(new URI("hdfs://master:8020/"), new Configuration())
      val path = new Path(HDFSStopPath)
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      stopFlag = hdfs.exists(path)
      if (!isStopped && stopFlag) {
        println("streaming stopping")
        ssc.stop(true, true)
        println("streaming stopped")
        hdfs.delete(path, true)
      }
    }
  }
}