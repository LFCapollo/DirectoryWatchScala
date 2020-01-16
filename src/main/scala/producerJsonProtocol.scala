
import spray.json._
//import DefaultJsonProtocol._

final case class Produce(key: String, name: String, surname: String)
final case class ProduceJson(msg: String)
object producerJsonProtocol extends DefaultJsonProtocol{
implicit val produceFormat: RootJsonFormat[Produce]= jsonFormat(Produce, "key", "name", "surname")
  implicit val produceFormatJson: RootJsonFormat[ProduceJson] = jsonFormat(ProduceJson, "msg")
}
