import org.apache.spark.sql.SparkSession
import java.io._
import scala.collection.immutable._
import org.graphframes.GraphFrame
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.collection.mutable.Set
import scala.io.Source


object task1 {
  def main(arg:Array[String]): Unit={

    val start = System.currentTimeMillis()
    val spark = SparkSession.builder().appName("Task1").config("spark.master", "local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /*
    Initializing
    */
    val threshold = arg(0).toInt
    val input_path = arg(1)
    val output_path = arg(2)

    val MAX_ITER = 5

    val user_bus_dict = scala.collection.mutable.Map[String, Set[String]]()

    for (line <- Source.fromFile(input_file).getLines().drop(1)) {
      val row = line.split(",")
      val user = row(0)
      val business = row(1)
      user_bus_dict(user) = user_bus_dict.getOrElse(user, Set[String]()) += business
    }

    var edges = new ListBuffer[(String, String)]()
    var vertices = Set[String]()

    for ((user1, user2) <- user_pair_list) {
      if ((user_bus_dict(user1) & user_bus_dict(user2)).size >= filter_threshold) {
        edges += ((user1, user2))
        edges += ((user2, user1))
        vertices += user1
        vertices += user2
      }
    }

    val edges_df = spark.createDataFrame(edges).toDF("src", "dst")
    val vertices_df = spark.createDataFrame(vertices.toSeq.map(Tuple1(_))).toDF("id")

    val graph = GraphFrame(vertices_df, edges_df)
    val communities = graph.labelPropagation.maxIter(MAX_ITER).run()

    val communities_rdd = communities.rdd.map(x => (x.getLong(1), x.getString(0))).groupByKey()
      .map(x => x._2.toList.sorted).sortBy(community => (community.length, community))

    communities_rdd.map(community => community.mkString("[", ", ", "]"))
      .coalesce(1, shuffle = true)
      .saveAsTextFile(output_file)

    println(System.currentTimeMillis() - start)

    spark.stop()
  }
}