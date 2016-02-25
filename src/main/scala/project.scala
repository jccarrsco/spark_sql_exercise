import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/*
* Written by Jose Carlos Carrasco Jimenez
* Purpose: 1) Create a Social Network for an individual based on email communication patterns
*          2) Create a Social Network for all the email communications
*/

object Project extends App {
	val spConfig = (new SparkConf)
									.setMaster("local")
									.setAppName("SparkSQLProject")

		val sc = new SparkContext(spConfig)
		val sqlContext = new SQLContext(sc)	// initialize SQL context

		/*
		*	Load from an external data source (json)
		*/
		val inputFile = "path_to_data"
		val emails = sqlContext.read.json(inputFile)

// ***************************************************************************************************************
// PART 1
		// COMMON DataFrame OPERATIONS

		
		
		

// ***************************************************************************************************************
// PART 2
		/*
		* EXTRACT THE SOCIAL NETWORK OF A GIVEN USER
		*/
		val user = "\"rosalee.fleming@enron.com\""	// make sure to escape the string

		val rosalee = Network.userNetwork(user, sqlContext, emails)	// defined in Network

		rosalee.collect().foreach(println)

		// Send network of a user to a file
		//val rosaleeReady = rosalee.map{case (s, r, c) => s.replace("\"","") + " " + r + " " + c}


// ***************************************************************************************************************
// PART 3


		/*
		* EXTRACT THE WHOLE SOCIAL NETWORK
		*/
		// we already have senders in from but we need to convert to RDD
		import org.apache.spark.sql.Row	// df.rdd returns an RDD of "Row"

		// get a list of senders and convert to RDD of users (String)
		val senders = from.rdd
											.map{case Row(f) => f.asInstanceOf[String]}

		// get a list of receivers so we can get a list of all nodes of the network
		val receivers = to.map{case Row(to) => to}	//extract to
											.flatMap( r => r.asInstanceOf[Seq[String]]) // convert to a flat list of Strings

		/********************************
		* NODES
		********************************/
		// Create a list of the nodes of the network
		val nodes = senders.union(receivers)
		println("Nodes: " + nodes.count)

		val uniqueNodes = nodes.distinct	// get unique users (nodes)
													 .zipWithIndex()	// add the node index

		println("Unique Nodes: " + uniqueNodes.count)

		// Send nodes to a file
		val nodesFile = "src/main/resources/nodes"

		// prepare nodes for Pajek format
		val uniqueNodesPajek = uniqueNodes.map{case (node, index) => (index+1) + " " + node}	// start index from 1 and place in right order
		uniqueNodesPajek.coalesce(1).saveAsTextFile(nodesFile)


		/********************************
		* EDGES
		********************************/
		// get a list of the senders
		val uniqueSenders = senders.distinct.take(200) // if we use collect we overload a single machine

		// construct the edges of the whole network
		val reducedEdges = Network.wholeNetwork(uniqueSenders, sqlContext, emails)
		reducedEdges.collect().foreach(println)

		// create a mapping from email to node_id so we can convert to Pajek format
		val emailToNodeID = uniqueNodes.map{case (n, i) => (n, (i+1))}.collectAsMap()

		// Prepare edges to be sent to a file
		val reducedEdgesReady = reducedEdges.map{case (s, r, c) =>
																								emailToNodeID.getOrElse(s.replace("\"", ""), 0) + " " +
																								emailToNodeID.getOrElse(r, 0) + " " + c }

		// send edges to file
		val edgesFile = "src/main/resources/edges"
		reducedEdgesReady.coalesce(1).saveAsTextFile(edgesFile)

		/********************************
		* Get the nodes and edges and build a Pajek Network
		********************************/

		val netFile = "src/main/resources/mail.net"
		Network.createPajekNetwork(nodesFile + "/part-00000",
															 edgesFile + "/part-00000",
															 netFile)

}
