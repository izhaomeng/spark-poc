package rmdsystem.scala

import play.api.libs.json._
import play.api.libs.functional.syntax._
import org.scalatest.junit.JUnitSuite
import org.junit.Test

class ScalaTest extends JUnitSuite {
  
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
  
  @Test
  def testJson() {
    var meegoJsonString = """
      {"name":"zhaomeng","job":"arte", "jobYears": 5}
      """
    var meegoJson = Json.parse(meegoJsonString)
    var meego = Json.fromJson(meegoJson)
    println(meego.get.getClass)

    
    var mo = Meego("zhaomeng","prog",4)
    val moJson = Json.toJson(mo)
    val moJsonString = moJson.toString()
    println(moJsonString)
    
  }
  

  case class Person(var name: String, var sex: String, var weight: Integer) {
//    def toXml = {
//      <person>
//				<name>{ name }</name>
//				<sex>{ sex }</sex>
//				<weight>{ weight }</weight>
//			</person>
//    }
//
//    override def toString =
//      s"name: $name, sex: $sex, weight: $weight"
  }

  object Person {
    def toXml(person: Person) = {
      <person>
				<name>{ person.name }</name>
				<sex>{ person.sex }</sex>
				<weight>{ person.weight }</weight>
			</person>
    }

    def fromXml(node: scala.xml.Node): Person = {
      val name = (node \ "name").text
      val sex = (node \ "sex").text
      val weight = (node \ "weight").text.toInt
      new Person(name, sex, weight)
    }
  }
  
  @Test
  def testXml() {
      var mzXmlString = """
                  <person>
            				<name>zhaomeng</name>
            				<sex>male</sex>
            				<weight>131</weight>
            			</person>
            			"""
      var mzXml = scala.xml.XML.loadString(mzXmlString)
      var mz = Person.fromXml(mzXml)
      println(mz)
      
      var zm = new Person("zhaomeng", "male", 130)
      var zmXml = Person.toXml(zm)
      var zmXmlString = zmXml.toString()
      println(zmXmlString)
    }
  
  @Test
  def fileio() = {
      var stringSource = scala.io.Source.fromString("a,b,c,1")
      stringSource.getLines().foreach { line => line.split(",").foreach(println) }
      
      
      var localFileSource = scala.io.Source.fromFile("data/stream.txt")
      localFileSource.getLines().foreach { line => line.split(",").foreach(println) }
      
      
      var internetFileSource = scala.io.Source.fromURL("https://www.baidu.com/index.html", "utf-8")
      internetFileSource.getLines().foreach { line => line.split(",").foreach(println) }
      
      
      var printer = new java.io.PrintStream(new java.io.File("data/out.txt"))
      stringSource.getLines().foreach { line => printer.append("\"").append(line).append("\"") }
      printer.flush()
      printer.close()
  }
  
}