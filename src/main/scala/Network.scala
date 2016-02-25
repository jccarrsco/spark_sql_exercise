import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

object Network{

  //Extract the social network of a given sender
  def userNetwork(user: String, sqlContext: SQLContext, emails: DataFrame) = {

    
    // return RDD of (user, receiver, count)
    
  }

  //Extract the social network of all the nodes at once
  def wholeNetwork(users: Iterable[String], sqlContext: SQLContext, emails: DataFrame) = {
    // Iterate each user and pass it to the "userNetwork" function. Make sure you escape string users

    // return reducedEdges
  }

  // Construct the Pajek network so that it can be analyzed
  import scala.io.Source
  import java.io.FileWriter
  def createPajekNetwork(nodesFile: String, edgesFile: String, netFile: String) {
    // get the nodes and count the number of nodes
    val nodes = Source.fromFile(nodesFile).getLines().toList
    val numNodes = nodes.size

    // get the edges
    val edges = Source.fromFile(edgesFile).getLines().toList

    println("printing lines from edges")
    edges.take(3).foreach(println)

    // create a file with ".net" extension
    val fw = new FileWriter(netFile, true)

    try{
      fw.write("*Vertices " + numNodes + "\n")

      // send the nodes to the file
      nodes.map(l => if(l.trim != "") fw.write(l + "\n"))

      // Now send the edges to the file
      fw.write("*arcs \n")
      edges.map(l => if(l.trim != "") fw.write(l + "\n"))
    } finally{
      fw.close()
    }
  }

}
