import CoTrajectoryUtils._
import Swapmob._
import org.apache.spark.sql.Dataset
import org.apache.spark.graphx._
import java.io._
import scala.collection.JavaConversions._

class ExampleSynthCDRLatLngTest extends org.scalatest.FunSuite {
  
  test("ExampleSynthCDRLatLng.runExample") {
    val numTrajectories = ExampleSynthCDRLatLng.runExample
    assert(numTrajectories == 18496)
  }
  
  test("ExampleSynthCDRLatLng.runGridSizeTimeTest") {
    val numMeasurements = ExampleSynthCDRLatLng.runGridSizeTimeTest
    assert(numMeasurements == 242217)
  }
}
