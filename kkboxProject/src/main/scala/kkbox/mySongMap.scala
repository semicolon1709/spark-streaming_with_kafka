package kkbox

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object mySongMap {
  @volatile private var instance: Broadcast[Map[String, String]] = null

  def getInstance(sc: SparkContext): Broadcast[Map[String, String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val songInfo = sc.textFile("hdfs://master:8020/kkbox/songs.csv/songs.csv")
          val songMap = songInfo.map(row => (row.split(",")(0),row.split(",")(3))).collect().toMap
          instance = sc.broadcast(songMap)
        }
      }
    }
    instance
  }
}
