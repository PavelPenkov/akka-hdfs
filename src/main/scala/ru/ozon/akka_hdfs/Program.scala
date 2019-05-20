package ru.ozon.akka_hdfs

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.alpakka.hdfs.{FilePathGenerator, FileUnit, HdfsWriteMessage, HdfsWritingSettings, RotationMessage, RotationStrategy, SyncStrategy, WrittenMessage}
import akka.stream.alpakka.hdfs.scaladsl.HdfsFlow
import akka.stream.scaladsl._
import akka.util.ByteString
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.{DefaultCodec, SnappyCodec}
import org.apache.kafka.common.serialization.{ByteBufferDeserializer, StringDeserializer}
import org.slf4j.LoggerFactory
import sun.misc.{Signal, SignalHandler}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Program extends App {
  val event = scala.io.Source.fromResource("event.json").mkString
  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system = ActorSystem("akka-hdfs")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val fakeTracker = Source.fromIterator(() => Iterator.continually(event))

  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://nn01.hdp.dwh.o3.ru:8020")
  conf.set("dfs.client.use.datanode.hostname", "true")

  val fs = FileSystem.get(conf)

  val sync = SyncStrategy.none

  val rotation = RotationStrategy.time(60 seconds)

  val path = FilePathGenerator((rotationCount: Long, timestamp: Long) => s"/tmp/hdfs-bench/test-$rotationCount-$timestamp")

  val settings = HdfsWritingSettings().withPathGenerator(path)
    .withNewLine(false)

  val flow = HdfsFlow.data(fs, sync, rotation, settings)

  val kafkaSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)

  val tracker = Consumer.committableSource(kafkaSettings, Subscriptions.topics("ozon-tracker"))

  val codec = new SnappyCodec()
  codec.setConf(fs.getConf)

  val compressedFlow = HdfsFlow.compressedWithPassThrough[CommittableOffset](fs, sync, rotation, codec, settings)

  val sep = System.getProperty("line.separator").getBytes

  val done = tracker
    .buffer(50 * 1000, OverflowStrategy.backpressure)
    .map { msg =>
      val bs = ByteString(msg.record.value().getBytes() ++ sep)
      HdfsWriteMessage(bs, msg.committableOffset)
    }
    .via(compressedFlow).async
    .splitAfter(_.isInstanceOf[RotationMessage])
    .fold(CommittableOffsetBatch.empty) {
      case (batch, WrittenMessage(passThrough, inRotation)) => batch.updated(passThrough)
      case (batch, RotationMessage(_, _)) => batch
    }
    .mergeSubstreams
    .mapAsync(1)(_.commitScaladsl())
    .runWith(Sink.ignore)

  done onComplete  { t =>
    t match {
      case Success(_) =>
        logger.info("Finished successfully")
      case Failure(e) =>
        logger.error(s"Finished with error ${e.getMessage}", e)
    }

    sys.exit()
  }

  Seq("INT", "TERM").foreach { s =>
    Signal.handle(new Signal(s), (signal: Signal) => {
      logger.info(s"Received signal ${signal.getName}, exiting...")
      sys.exit(0)
    })
  }
}
