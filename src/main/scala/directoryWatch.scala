import java.io.{File, FileNotFoundException, IOException}
import java.nio.charset.UnsupportedCharsetException
import java.nio.file.{FileSystems, Files, Path, Paths, StandardCopyOption}
import java.util.Properties
import java.util.concurrent.TimeUnit
import com.typesafe.scalalogging.LazyLogging

import scala.util.Failure
import akka.Done
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSON
import scala.util.Success
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import spray.json._
import producerJsonProtocol._
import akka.stream.alpakka.file.scaladsl.DirectoryChangesSource

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.io.BufferedSource
object directoryWatch extends LazyLogging {
  val log: Logger = LoggerFactory.getLogger(this.getClass)
  if (!(java.nio.file.Files.exists(java.nio.file.Paths.get("src\\main\\resources\\producerProperties.conf"))) ){
    log.error("producerProperties could not be found ")
    println("producerProperties could not be found ")
    System.exit(0)
  }
  //Check existence of dirProperties config file
  if (!(java.nio.file.Files.exists(java.nio.file.Paths.get("src\\main\\resources\\directoryProperties.conf"))) ){
    println("dirProperties could not be found ")
    log.error("dirProperties could not be found ")
    System.exit(0)
  }
  //load config files
  val config = ConfigFactory.load("directoryProperties")
  val pConfig = ConfigFactory.load("producerProperties")

  //Creating ActorSystem and actor Materializer
  implicit val system = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  //checking existence of important configs
  if ((!(pConfig.hasPath("p.topic")) || !(pConfig.getString("p.topic") !="") )
    || (!(pConfig.hasPath("p.bootstrap")) || !(pConfig.getString("p.bootstrap") !=""))
    || (!(pConfig.hasPath("p.key")) || !(pConfig.getAnyRef("p.key") !=""))
  ){
    println("Missing important config (topic,  bootstrap) in producerProperties")
    log.error("Missing important config (topic,  bootstrap) in producerProperties")
    System.exit(0)
  }
  val key = pConfig.getAnyRef("p.key").toString
  val props =new Properties()
  //configuring kafka
  val bootstrapServers = pConfig.getString("p.bootstrap")
  val topic = pConfig.getString("p.topic")
  val conf = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings =
    ProducerSettings(conf, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)

  props.put("bootstrap.servers",pConfig.getString("p.bootstrap1"))
  props.put("key.serializer", pConfig.getString("p.key.serializer"))
  props.put("value.serializer", pConfig.getString("p.value.serializer"))

  //checkinf if number of columns is declared
  if (!(config.hasPath("D.numCols")) || !(config.getString("D.numCols") !="") ){
    println("Please check numCols in dirProperties.conf")
    log.warn("Please check numCols in dirProperties.conf")
  }

  var numCols: Int = config.getInt("D.numCols")

  //check existence of directories
  if (!(new File(config.getString("D.path")).exists()) || !(new File(config.getString("D.path")).isDirectory())){
    println("Directory to watch does not exist")
    log.error("Directory to watch does not exist")
    System.exit(0)
  }
  else if(!(new File(config.getString("D.sent")).exists()) || !(new File(config.getString("D.sent")).isDirectory())){
    println("Directory sent does not exist")
    log.error("Directory sent does not exist")
    System.exit(0)
  }
  else if(!(new File(config.getString("D.notSent")).exists()) || !(new File(config.getString("D.notSent")).isDirectory())){
    println("Directory  notSent does not exist")
    log.error("Directory  notSent does not exist")
    System.exit(0)
  }

  var destinationDirectory = config.getString("D.notSent")

  /**
   * moeFile moves file from 1 directory to another
   * to be used in the future
   *
   * @param source
   * @param destination
   */
  def moveFile(source: String, destination: String): Unit = {
    try {
      val path = Files.move(
        Paths.get(source),
        Paths.get(destination),
        StandardCopyOption.REPLACE_EXISTING
      )
    }
    catch {
      case ex: FileNotFoundException => {
        println("file not found in function moveFile")
        log.error("file not found in function moveFile")

      }
      case unknown => {
        println("Unknown can not move files" + source)
        log.error("can not move files" + source)
      }
    }
//   println("file: " + source + " is moved to "+ destination)
//    log.info("file: " + source + " is moved to "+ destination)
  }
  /**
   * list files from directory
   * @param dir
   * @return list of files in directory
   */
  def getListOfFiles(dir: String): List[String] = {
    //checking if directory exists
    if(!(new File(dir).isDirectory())){
      System.exit(0)
    }
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      //.filter(_.getName.endsWith("csv"))
      .map(_.getPath).toList
  }
  /**
   * checks whether rows are consistent
   * @param file
   * @return
   */
  def checkColumns(file: String): Boolean = {
    //checking if file exists
//    if(!(new File(file).exists())){
//      println("file not found in checkColumns")
//      return false
//    }
    val f = io.Source.fromFile(file)
    for (line <- f.getLines()) {
      var tokens=line.split(",").map(_.trim)
      if (tokens.length != numCols){
        println("check columns in: " + file.toString)
        log.info("check columns in: " + file.toString)
        f.close()
        return false
      }
    }
    f.close()
    return true
  }
def checkTempFolder(): Unit = {
  val tempFiles: List[String] = getListOfFiles(config.getString("D.path"))
  for (file <- tempFiles) {
    moveFile(file, config.getAnyRef("D.path").toString() + file.slice(file.lastIndexOf("\\") + 1, file.length))
  }
}

  def main(args: Array[String]): Unit = {
//    val filesInDirectory: List[String] =getListOfFiles(config.getString("D.path"))
//    for (file <- filesInDirectory){
//      moveFile(file, config.getAnyRef("D.tempFolder").toString() + file.slice(file.lastIndexOf("\\") + 1, file.length))
//    }

    Future.traverse(getListOfFiles(config.getString("D.path"))) { file =>

      println(s"[${Thread.currentThread().getName}]-sending $file to process")

      processData(file)

    } onComplete {
      //when thread is executed successfully
      case Success(processed) => {


        processed.foreach(p => {
          println(s"""[${Thread.currentThread().getName}]-$p""")
        })

      }
      case Failure(f) => f.getMessage


    }

    /**
     * monitoring File system
     * directory path retrieved from directoryProperties config file
     * changes contain path of new file and change type (create, delete, modify)
     */
    val fs = FileSystems.getDefault
    val changes = DirectoryChangesSource(fs.getPath(config.getString("D.path")), pollInterval = FiniteDuration(1, TimeUnit.SECONDS), maxBufferSize = 1000)

    changes.runForeach {

      case (path, change) => {


        destinationDirectory = config.getString("D.notSent")
       // println("sent to checkColumns")
        //var bool : Boolean =checkColumns(path.toString)

       // println("return from checkColumn " + bool)
    if ((change.toString() == "Creation") && (path.toString().takeRight(3) == "csv" )) {
      println("file: " +path + " is being processed")
      log.info("file: " +path + " is being processed")
          val producerDone: Future[Done] = {
            FileIO.fromPath(path)
              // Separate each line by standard line terminator to be processed individually by flow
              .via(Framing.delimiter(ByteString("\n"), 256, allowTruncation = true))
              .map { line =>
                // Convert line to JSON string and send it to Kafka broker
                val cols = line.utf8String.split(",").map(_.trim)
                val produce = Produce(cols(0).toString, cols(1).toString, cols(2).toString)
                new ProducerRecord[String, String](topic, key, produce.toJson.compactPrint)
              }
              // Enable error logging in console
              .log("error logging")
              // Send stream to Kafka producer sink
              .runWith(Producer.plainSink(producerSettings))
            }
            producerDone.onComplete{
              //when stream is complete, files are moved to sent directory
              case Success(succ) => {
                destinationDirectory = config.getString("D.sent")

                moveFile(path.toString, destinationDirectory.toString() + path.toString.slice(path.toString.lastIndexOf("\\") + 1, path.toString.length))
                println("file: " + path + " is moved to "+ destinationDirectory)
                log.info("file: " + path + " is moved to "+ destinationDirectory)
              }
                //file is moved to notSent
              case Failure(f)=>{
                f.getMessage()
                moveFile(path.toString, destinationDirectory.toString() + path.toString.slice(path.toString.lastIndexOf("\\") + 1, path.toString.length))
                println("file: " + path + " is moved to "+ destinationDirectory)
                log.info("file: " + path + " is moved to "+ destinationDirectory)
              }
            }


           // println("done")
           //estinationDirectory = config.getString("D.sent")
      // moveFile(path.toString, destinationDirectory.toString() + path.toString.slice(path.toString.lastIndexOf("\\") + 1, path.toString.length))
          }
       else if ((change.toString() == "Creation") && (path.toString().takeRight(4) == "json" )) {
      println("file: " +path + " is being processed")
      log.info("file: " +path + " is being processed")

          val producerDone: Future[Done] = {
            FileIO.fromPath(path)
              // Separate each line by standard line terminator to be processed individually by flow
              .via(Framing.delimiter(ByteString("},"), 8192, allowTruncation = true))
              .map { line =>
                val cols = line.utf8String
                val produceJson = ProduceJson(cols)
                //println(produce)
                // Convert line to JSON string and send it to Kafka broker
                //val cols = line.utf8String.split(",").map(_.trim)
                //val produce = Produce(cols(0).toString, cols(1).toString, cols(2).toString)
                new ProducerRecord[String, String](topic, key, produceJson.toJson.compactPrint)
              }
              // Enable error logging in console
              .log("error logging")
              // Send stream to Kafka producer sink
              .runWith(Producer.plainSink(producerSettings))
          }
          producerDone.onComplete{
            //when stream is complete, files are moved to sent directory
            case Success(succ) => {
              destinationDirectory = config.getString("D.sent")
              moveFile(path.toString, destinationDirectory.toString() + path.toString.slice(path.toString.lastIndexOf("\\") + 1, path.toString.length))
              println("file: " + path + " is moved to "+ destinationDirectory)
              log.info("file: " + path + " is moved to "+ destinationDirectory)
            }
            //file is moved to notSent
            case Failure(f)=>{
              f.getMessage()
              moveFile(path.toString, destinationDirectory.toString() + path.toString.slice(path.toString.lastIndexOf("\\") + 1, path.toString.length))
              println("file: " + path + " is moved to "+ destinationDirectory)
              log.info("file: " + path + " is moved to "+ destinationDirectory)
            }
          }


          // println("done")
          //estinationDirectory = config.getString("D.sent")
          // moveFile(path.toString, destinationDirectory.toString() + path.toString.slice(path.toString.lastIndexOf("\\") + 1, path.toString.length))
        }
else if (change.toString() == "Creation")  {
   moveFile(path.toString, destinationDirectory.toString() + path.toString.slice(path.toString.lastIndexOf("\\") + 1, path.toString.length))
      println("file: " + path + " is moved to "+ destinationDirectory)
      log.info("file: " + path + " is moved to "+ destinationDirectory)
          //println(destinationDirectory.toString() + path.toString.slice(path.toString.lastIndexOf("\\") + 1, path.toString.length))

 // println(path.toString)
}

        }
        }
      }


  def processData: (String => Future[String]) = file => {
    Future {
      var location="notSent" //to print in console whether file is sent or not
      var f : BufferedSource = null
      //var bool : Boolean =checkColumns(file)  //checking files before processing
      //if((file.toString.takeRight(3)=="csv") && bool) { //only csv files are processed
        if(file.toString.takeRight(3)=="csv" && checkColumns(file))  {
          try {
          val producer = new KafkaProducer[String, String](props)
          f = io.Source.fromFile(file)
          for (line <- f.getLines()) {
            var tokens = line.split(",").map(_.trim)
            var msg : String = ""
            tokens.foreach(token => msg += token + ", ")
            val record = new ProducerRecord[String, String](topic, key, msg.toString)
            producer.send(record)
          }
          f.close()
          producer.close()
          location = "sent"
          moveFile(file, config.getString("D.sent") + file.slice(file.lastIndexOf("\\") + 1, file.length))
        }

        catch {

          case ex: FileNotFoundException =>
            f.close()
            ex.printStackTrace()
          case ex: UnsupportedCharsetException =>
            f.close()
            moveFile(file, config.getString("D.notSent") + file.slice(file.lastIndexOf("\\") + 1, file.length))
            ex.printStackTrace()
          case ex: IOException =>
            f.close()
            moveFile(file, config.getString("D.notSent") + file.slice(file.lastIndexOf("\\") + 1, file.length))
            ex.printStackTrace()
          case unknown => println("Got this unknown exception: " + unknown)
            f.close()
            moveFile(file, config.getString("D.notSent") + file.slice(file.lastIndexOf("\\") + 1, file.length))
        }
        finally {
          f.close()
        }
      }
     else if(file.toString.takeRight(4)=="json")  {
        try {
          val producer = new KafkaProducer[String, String](props)
          f = io.Source.fromFile(file)
          val json_content = scala.io.Source.fromFile(file).mkString
          val json_data = JSON.parseFull(json_content)
          json_data match {
            case Some(e) => {

              var tokens = e.toString.split("""\),""").map(_.trim)
              println()
              tokens.foreach(msg =>{
                val record = new ProducerRecord[String, String](topic, key, msg)
                producer.send(record)
              })
              producer.close()
              location = "sent"
              f.close()
              moveFile(file, config.getString("D.sent") + file.slice(file.lastIndexOf("\\") + 1, file.length))
            }
            case None => println("failed")
          }

        }

        catch {

          case ex: FileNotFoundException =>
            f.close()
            ex.printStackTrace()
          case ex: UnsupportedCharsetException =>
            f.close()
            moveFile(file, config.getString("D.notSent") + file.slice(file.lastIndexOf("\\") + 1, file.length))
            ex.printStackTrace()
          case ex: IOException =>
            f.close()
            moveFile(file, config.getString("D.notSent") + file.slice(file.lastIndexOf("\\") + 1, file.length))
            ex.printStackTrace()
          case unknown => println("Got this unknown exception: " + unknown)
            f.close()
            println("unknown")
            moveFile(file, config.getString("D.notSent") + file.slice(file.lastIndexOf("\\") + 1, file.length))
        }
        finally {
          f.close()
        }
      }
      else {
          println("else")
        moveFile(file, config.getString("D.notSent") + file.slice(file.lastIndexOf("\\") + 1, file.length()))
      }
      s"[Thread-${Thread.currentThread().getName}] has moved file $file in $location."

    }
  }
}

