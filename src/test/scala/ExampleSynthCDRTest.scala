import CoTrajectoryUtils._
import Swapmob._
import org.apache.spark.sql.Dataset
import org.apache.spark.graphx._
import java.io._
import scala.collection.JavaConversions._

class ExampleSynthCDRTest extends org.scalatest.FunSuite {
  
  test("ExampleSynthCDR.runExample") {
    val numTrajectories = ExampleSynthCDR.runExample
    assert(numTrajectories == 18496)
  }
  
  test("ExampleSynthCDR.runGridSizeTimeTest") {
    val numMeasurements = ExampleSynthCDR.runGridSizeTimeTest
    assert(numMeasurements == 242217)
  }
}
