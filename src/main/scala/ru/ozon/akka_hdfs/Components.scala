package ru.ozon.akka_hdfs

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{Committable, CommittableOffset}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.stream.alpakka.hdfs.{FilePathGenerator, HdfsWritingSettings, OutgoingMessage, RotationMessage, RotationStrategy, SyncStrategy, WrittenMessage}
import akka.stream.alpakka.hdfs.scaladsl.HdfsFlow
import akka.stream.scaladsl.{Flow, Source}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec, SnappyCodec, ZStandardCodec}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.language.postfixOps
import scala.concurrent.duration._

object Components {
  val topic = "tracker.prod.flatten"

  def trackerSource(system: ActorSystem) = {
    val settings = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers("datalake.kafka.s.o3.ru:9092")
      .withGroupId("akka-hdfs-benchmark")
    Consumer.committableSource(settings, Subscriptions.topics(topic))
  }

  def simpleSource(system: ActorSystem) = {
    val settings = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers("datalake.kafka.s.o3.ru:9092")
      .withGroupId("akka-hdfs-benchmark")

    Consumer.plainSource(settings, Subscriptions.topics(topic))
  }

  def committer(system: ActorSystem) = {
    val settings = CommitterSettings(system)
      .withParallelism(1)
      .withMaxInterval(30 seconds)

    Committer.sink(settings)
  }


  val hdfsConf = {
    val conf = new Configuration()
    conf.set("fs.default.name", "hdfs://nn02.hdp.dwh.o3.ru:8020")
    conf.set("ipc.client.connect.timeout", "1000")
    conf.set("dfs.client.failover.max.attempts", "3")
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    conf
  }

  val fs = FileSystem.get(hdfsConf)

  private val rng = new scala.util.Random()

  val pathGenerator = FilePathGenerator { case (_, timestamp)  =>
    val suffix = rng.alphanumeric.take(6).mkString
    s"/tmp/akka_hdfs_benchmark_${timestamp}_${suffix}" }

  val writingSettings = HdfsWritingSettings().withNewLine(false)
    .withOverwrite(true)
    .withPathGenerator(pathGenerator)

  def hdfsFlowGz = {
    val codec = new GzipCodec()
    codec.setConf(hdfsConf)

    HdfsFlow.compressed(fs,
      SyncStrategy.none,
      RotationStrategy.time(1 minute),
      codec,
      writingSettings
    )
  }

  def hdfsFlowGzWithPassThrough = {
    val codec = new GzipCodec()
    codec.setConf(hdfsConf)

    HdfsFlow.compressedWithPassThrough[CommittableOffset](fs,
      SyncStrategy.none,
      RotationStrategy.time(1 minute),
      codec,
      writingSettings
    )
  }

  def hdfsFlowUncompressed = {
    HdfsFlow.data(fs,
      SyncStrategy.none,
      RotationStrategy.time(1 minute),
      writingSettings
    )
  }

  def hdfsFlowSnappy = {
    val codec = new SnappyCodec()
    codec.setConf(hdfsConf)

    HdfsFlow.compressed(fs,
      SyncStrategy.none,
      RotationStrategy.time(1 minute),
      codec,
      writingSettings
    )
  }

  def hdfsFlowIntel = {
    val codec = new com.intel.compression.hadoop.IntelCompressionCodec()
    hdfsConf.set("io.compression.codec.intel.codec", "igzip")
    codec.setConf(hdfsConf)

    HdfsFlow.compressed(fs,
      SyncStrategy.none,
      RotationStrategy.time(1 minute),
      codec,
      writingSettings
    )
  }

  def hdfsFlowIntelPassthrough = {
    val codec = new com.intel.compression.hadoop.IntelCompressionCodec()
    hdfsConf.set("io.compression.codec.intel.codec", "igzip")
    codec.setConf(hdfsConf)

    HdfsFlow.compressedWithPassThrough[CommittableOffset](fs,
      SyncStrategy.none,
      RotationStrategy.time(1 minute),
      codec,
      writingSettings
    )
  }

  def hdfsFlowZstd = {
    val codec = new ZStandardCodec()
    codec.setConf(hdfsConf)

    HdfsFlow.compressed(fs,
      SyncStrategy.none,
      RotationStrategy.time(1 minute),
      codec,
      writingSettings
    )
  }

  def stringSource = {
    val eventBytes = scala.io.Source.fromResource("event.json").mkString.getBytes()
    Source.fromIterator(() => Iterator.continually(eventBytes))
  }

  val traceFlow = {
    var count = 0
    Flow[OutgoingMessage[_]].map { msg =>
      msg match {
        case RotationMessage(path, _) =>
          println(s"Wrote file $path, count = $count")
          count = 0
        case WrittenMessage(passThrough, inRotation) => count+=1
      }
      msg
    }
  }
}
