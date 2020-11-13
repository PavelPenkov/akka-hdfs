package ru.ozon.akka_hdfs

import java.io.FileOutputStream
import java.lang.System.LoggerFinder
import java.nio.ByteBuffer
import java.nio.file.FileSystems
import java.util
import java.util.Properties
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.zip.GZIPOutputStream

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{Committable, CommittableOffsetBatch}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.alpakka.hdfs.{HdfsWriteMessage, OutgoingMessage, RotationMessage, RotationStrategy, SyncStrategy, WrittenMessage}
import akka.stream.alpakka.hdfs.scaladsl.HdfsFlow
import akka.stream.scaladsl.{Flow, Keep, PartitionHub, Sink, Source}
import akka.util.ByteString
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.compress.{GzipCodec, SnappyCodec, ZStandardCodec}
import org.apache.hadoop.io.compress.zlib.ZlibCompressor
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteBufferDeserializer
import org.slf4j.LoggerFactory
import ru.ozon.akka_hdfs.Components.{fs, hdfsConf, writingSettings}
import ru.ozon.akka_hdfs.NoCommitSnappy.graph
import sun.misc.{Signal, SignalHandler}

import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.jdk.FunctionConverters._
import scala.language.postfixOps
import scala.util.{Failure, Success}
import scala.concurrent.duration._

trait Common {
  implicit val system: ActorSystem = ActorSystem()
  implicit val dispatcher = system.dispatcher

  def registerSignals = {
    Seq("INT", "TERM").foreach { s =>
      Signal.handle(new Signal(s), (s: Signal) => {
        println(s"Received signal $s, shutting down...")
        sys.exit(0)
      })
    }
  }

  val newLine: ByteString = ByteString.fromArray("\n".getBytes)
}

object NoCommitGz extends App with Common {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val src = Components.trackerSource(system)
  val hdfs = Components.hdfsFlowGzWithPassThrough

  val graph = src.map { msg =>
    val bs = ByteString.fromArrayUnsafe(msg.record.value()).concat(newLine)
    HdfsWriteMessage(bs, msg.committableOffset)
  }.via(hdfs)

  val counter = Sink.fold[Int, OutgoingMessage[Committable]](0) { (acc, _) => acc + 1 }

  val (control, done) = graph.toMat(counter)(Keep.both).run()

  val start = System.currentTimeMillis()

  Seq("INT", "TERM").foreach { signal =>
    Signal.handle(new Signal(signal), (sig: Signal) => {
      logger.info(s"Received $sig, shutting down...")
      control.drainAndShutdown(done).onComplete { result =>
        result match {
          case Success(count) =>
            val duration = (System.currentTimeMillis() - start).toDouble / 1000
            val rps = count.toDouble / duration
            logger.info(s"Wrote $count records in $duration seconds for ${rps} msg/sec")
          case Failure(e) => logger.error(s"Completed with error $e", e)
        }
        logger.info("Terminating actor system")
        system.terminate()
      }
    })
  }
}


object NoCommitIntel extends App with Common {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val src = Components.trackerSource(system)
  val hdfs = Components.hdfsFlowIntelPassthrough

  val start = System.currentTimeMillis()

  val graph = src.map { msg =>
    val bs = ByteString.fromArrayUnsafe(msg.record.value()).concat(newLine)
    HdfsWriteMessage(bs, msg.committableOffset)
  }.via(hdfs)

  val counter = Sink.fold[Int, OutgoingMessage[Committable]](0) { (acc, _) => acc + 1 }

  val (control, done) = graph.toMat(counter)(Keep.both).run()

  Seq("INT", "TERM").foreach { signal =>
    Signal.handle(new Signal(signal), (sig: Signal) => {
      logger.info(s"Received $sig, shutting down...")
      control.drainAndShutdown(done).onComplete { result =>
        result match {
          case Success(count) =>
            val duration = (System.currentTimeMillis() - start).toDouble / 1000
            val rps = count.toDouble / duration
            logger.info(s"Wrote $count records in $duration seconds for ${rps} msg/sec")
          case Failure(e) => logger.error(s"Completed with error $e", e)
        }
        logger.info("Terminating actor system")
        system.terminate()
      }
    })
  }
}

object NoCommitZstd extends App with Common {
  val src = Components.trackerSource(system)
  val hdfs = Components.hdfsFlowZstd

  val graph = src.map { msg =>
    val bs = ByteString.fromArrayUnsafe(msg.record.value()).concat(newLine)
    HdfsWriteMessage(bs)
  }.via(hdfs)
    .via(Components.traceFlow)

  val (_, done) = graph.toMat(Sink.ignore)(Keep.both).run()

  done onComplete {
    case Success(_) => println("Finished successfullt")
    case Failure(e) => println(s"Finished with error $e")

  }
}

object NoCommitUncompressedAsync extends App with Common {
  val src = Components.trackerSource(system)
  val hdfs = Components.hdfsFlowUncompressed

  val graph = src.async.map { msg =>
    val bs = ByteString.fromArray(msg.record.value()).concat(newLine)
    HdfsWriteMessage(bs)
  }.via(hdfs)
    .via(Components.traceFlow)

  val (_, done) = graph.toMat(Sink.ignore)(Keep.both).run()

  done onComplete {
    case Success(_) => println("Finished successfullt")
    case Failure(e) => println(s"Finished with error $e")

  }
}

object NoCommitSnappy extends App with Common {
  val src = Components.trackerSource(system)
  val hdfs = Components.hdfsFlowSnappy

  val graph = src.map { msg =>
    val bs = ByteString.fromArray(msg.record.value()).concat(newLine)
    HdfsWriteMessage(bs)
  }.via(hdfs)
    .via(Components.traceFlow)

  val (_, done) = graph.toMat(Sink.ignore)(Keep.both).run()

  done onComplete {
    case Success(_) => println("Finished successfullt")
    case Failure(e) => println(s"Finished with error $e")

  }
}

object DevNull extends App with Common {
  val src = Components.trackerSource(system)

  val graph = src.to(Sink.ignore)

  graph.run
}

object JustHdfsGz extends App with Common {
  val src = Components.stringSource
  val hdfs = Components.hdfsFlowGz

  val graph = src.map(b => HdfsWriteMessage(ByteString.fromArray(b)))
    .via(hdfs)
    .via(Components.traceFlow)

  val (_, done) = graph.toMat(Sink.ignore)(Keep.both).run()

  done onComplete {
    case Success(_) => println("Finished successfullt")
    case Failure(e) => println(s"Finished with error $e")
  }
}

object FileToHdfs extends App with Common {
  registerSignals

  val fs = FileSystems.getDefault
  val hdfs = Components.hdfsFlowIntel

  val src = FileTailSource.lines(fs.getPath("events.json"), 32 * 1024, 1 second)
    .map(s => HdfsWriteMessage(ByteString(s).concat(newLine)))
    .via(hdfs)
    .via(Components.traceFlow)
    .runWith(Sink.ignore)
}

object Partitioned extends App with Common {
  registerSignals

  def createFlow(topic: String) = {
    val codec = new GzipCodec()
    codec.setConf(Components.hdfsConf)

    val hdfsFlow = HdfsFlow.compressed(Components.fs,
      SyncStrategy.none,
      RotationStrategy.time(1 minute),
      codec,
      writingSettings
    )
    Flow[Array[Byte]]
      .map(bs => HdfsWriteMessage(ByteString.fromArrayUnsafe(bs)))
      .via(Components.hdfsFlowGz)
  }

  val src = Source.fromIterator(() => Iterator.from(1))

  val parts = src.toMat(PartitionHub.sink(
    (size, elem) => elem % size,
    startAfterNrOfConsumers = 2,
    bufferSize = 256))(Keep.right)
    .run()
}

object JavaClient extends App with Common {
  private final val NEW_LINE = "\n".getBytes()
  val props = new Properties()
  props.setProperty("bootstrap.servers", "datalake.kafka.s.o3.ru:9092")
  props.setProperty("group.id", "akka-hdfs-test")
  props.setProperty("auto.offset.reset", "earliest")
  props.setProperty("enable.auto.commit", "false")
  props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000")
  props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "104857600")

  var count = 0
  val os = Components.fs.createFile(new Path("/tmp/hdfs_java/result.json"))
    .bufferSize(64 * 1024)
    .build()

  val codec = new GzipCodec()
  //  val codec = new SnappyCodec()
  //val codec = new com.intel.compression.hadoop.IntelCompressionCodec()
  //  val codec = new ZStandardCodec()
  hdfsConf.set("io.compression.codec.intel.codec", "igzip")
  hdfsConf.set("zlib.compress.level", args(0))
  hdfsConf.set("io.file.buffer.size", (64 * 1024).toString)
  codec.setConf(hdfsConf)

  val compOs = codec.createOutputStream(os)

  val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
  consumer.subscribe(Seq(Components.topic).asJava)

  var appStart = System.currentTimeMillis()
  var total = 0
  var size = 0L

  val gzip = new GZIPOutputStream(new FileOutputStream("tracker.gz"))

  sys.addShutdownHook {
    val duration = (System.currentTimeMillis() - appStart).toDouble / 1000
    val rps = total.toDouble / duration
    val bps = size.toDouble / duration
    println(s"Wrote $total records in $duration seconds for ${rps} msg/sec")
    println(s"Wrote $size bytes for ${bps} bytes/sec")
  }

  while (true) {
    val records = consumer.poll(java.time.Duration.ofMillis(100))
    val it = records.iterator()
    val start = System.currentTimeMillis()
    while (it.hasNext) {
      val msg = it.next()
      compOs.write(msg.value())
      compOs.write(NEW_LINE)
      size += msg.value().length
    }
    println(s"Wrote ${records.count()} records in ${System.currentTimeMillis() - start}")
    total += records.count()
  }
}