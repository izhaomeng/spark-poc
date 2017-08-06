package rmdsystem.scala

import play.api.libs.json._
import play.api.libs.functional.syntax._
import org.junit.Test

class MTest {

  @Test
  def testJson() {
    var jsStr = """
      {"name":"zhaomeng","sex":"male", "weight": 130}
      """
    var json = Json.parse(jsStr)
    println(Json.prettyPrint(json))

    case class Meego(name: String, job: String, jobYears: Int)

    implicit val meegoWrites = new Writes[Meego] {
      def writes(meego: Meego) = Json.obj(
        "name" -> meego.name,
        "job" -> meego.job,
        "jobYears" -> meego.jobYears)
    }

    implicit val meegoReads: Reads[Meego] = 
      (
          (JsPath \ "name").read[String] and 
          (JsPath \ "job").read[String] and 
          (JsPath \ "jobYears").read[Int]
      )(Meego.apply _)
      
    var mo = Meego("zhaomeng","eg",10)
      
    val json2 = Json.toJson(mo)
    println(Json.prettyPrint(json2))
    val jsStr2 = Json.fromJson(json2)
    println(jsStr2.get)
    
    
  }
}