import CoTrajectoryUtils._
import Swapmob._
import org.apache.spark.sql.Dataset
import org.apache.spark.graphx._
import java.io._
import scala.collection.JavaConversions._

object ExampleSynthCDRLatLng {
  
  val spark = SparkSessionHolder.spark
  import spark.implicits._

  def runExample() = {
    /* Open file for normal output */
    val output = new PrintWriter(new File("output/synthetic-cdr-test-latlng.txt"))

    /* Parse the co-trajectory */
    val cotraj: Dataset[Trajectory] = Parse
      .syntheticCDR("data/examples/cdr-synthetic-antennas.csv")
      .cache

    /* Compute number of trajectories and number of measurements */
    val numTrajectories: Long = cotraj.count
    val numMeasurements: Long = cotraj.map(_.measurements.length).reduce(_ + _)

    output.println("Number of trajectories: " + numTrajectories.toString)
    println("Number of trajectories: " + numTrajectories.toString)

    output.println("Number of measurements: " + numMeasurements.toString)
    println("Number of measurements: " + numMeasurements.toString)

    /* Compute possible swaps */ 
    val partitioning: (Long, Double) = (60L * 5, 1000L)
    val swaps: Dataset[Swap] = cotraj
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .cache

    val numSwaps: Long = swaps.count

    output.println("Number of possible swaps: " + numSwaps.toString)
    println("Number of possible swaps: " + numSwaps.toString)
    output.close()
        
    numTrajectories
  }

  
  def runGridSizeTimeTest() = {
    /* Open file for normal output */
    val output = new PrintWriter(new File("output/synthetic-cdr-grid-size-time-latlng.txt"))

    /* Parse the co-trajectory */
    val cotraj: Dataset[Trajectory] = Parse
      .syntheticCDR("data/examples/cdr-synthetic-antennas.csv")
      .cache

    /* Compute number of trajectories and number of measurements */
    val numTrajectories: Long = cotraj.count
    val numMeasurements: Long = cotraj.map(_.measurements.length).reduce(_ + _)

    output.println("Number of trajectories: " + numTrajectories.toString)
    println("Number of trajectories: " + numTrajectories.toString)

    output.println("Number of measurements: " + numMeasurements.toString)
    println("Number of measurements: " + numMeasurements.toString)

    /* Compute possible swaps for different values of grid's size (degrees) 
     * and time step (seconds)
     */ 
    output.println("Number of possible swaps for different grid size and time step")
    println("Number of possible swaps for different grid size and time step")

    output.println("Time step\tGrid size\tNumber of swaps")
    println("Time step\tGrid size\tNumber of swaps")

    for(timeStep <- Seq(5, 10, 15, 20, 25)) 
      for(gridSize <- Seq(1, 0.5, 0.25, 0.1, 0.075, 0.05, 0.01, 0.005)) {
        val partitioning: (Long, Double) = (60L * timeStep, gridSize)
        
        val swaps: Dataset[Swap] = cotraj
          .map(_.partitionDistinct(partitioning))
          .swaps(partitioning._1)
          .cache

        val numSwaps: Long = swaps.count

        output.println(Seq(timeStep, gridSize, numSwaps).mkString("\t"))
        println(Seq(timeStep, gridSize, numSwaps).mkString("\t"))
    }
    
    output.close()

    numMeasurements
  }
}
