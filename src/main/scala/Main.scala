import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
 * Created by yiucheungho on 5/9/15.
 */

object Main {
  def log2(x: Double) = scala.math.log(x)/scala.math.log(2)
  def main(args: Array[String]) {
    val logFile = "project3"
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile).take(2) //,2).cache()

    // Get line count
    val lineCount = sc.textFile(logFile).count().toDouble

    // Initialize arraybuffer for gene terms
    var geneArray = ArrayBuffer[(String, Double)]()

    // Get rid of tab, For each element, get rid of duplicate and write to new array
    logData.flatMap(line => line.split("\t")).foreach(a => {
      
      // Separate word for each document, only choose gene term without duplicates in single document
      val words = a.split(" ").filter(line => line.contains("gene_")).map(line => (line, 1.00)).distinct

      // For each document, we append all gene term into a big array for mapReduce functions later
      for ( i <- words){
        geneArray += i
      }
    })

    // Gene Term Count
    // Create RDDs: parallelizing collection, do calculation for each element
    val distData = sc.parallelize(geneArray).reduceByKey(_ + _)

    // Compute TF-IDF
    // Create vector for tf-idf for all gene term
    val geneTermFreq = sc.broadcast(distData.map( a => ( a._1,  (1/ a._2) * Math.log(lineCount / a._2)) ).cache())

//    // Calculate similarity for two vectors
//    def calSimilarity(a1_str:String,  a2_str:String, a1: RDD[(String, Double)], a2: RDD[(String, Double)]): Double = {
//      var count = 0.00
//      // Vector A
//      val vA = a1.filter(cur=>cur._1.contentEquals(a1_str)).map(x => x._2).cache().foreach( a => {
//        a2.filter(cur=>cur._1.contentEquals(a2_str)).map(x => x._2).cache().foreach( b => {
//          count = count + a * b
//        })
//      })
//      count
//    }

    // Calculate similarity, including sorting result
    val result = geneTermFreq.value.cartesian(geneTermFreq.value)
                            .map(a=> (a._1._1, a._2._1, a._1._2 * a._2._2 ))
                            .sortBy(a=>a._3, false)
    result.saveAsTextFile("result")
    //result.foreach(println)

  }
}
