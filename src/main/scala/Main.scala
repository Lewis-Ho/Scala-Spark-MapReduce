import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
 * Created by yiucheungho on 5/9/15.
 */

object Main {
  def ++[K, V](ts: Map[K, V], xs: Map[K, V]): Map[K, V] =
    (ts /: xs)  {case (acc, entry) =>
      println("acc = " + acc)
      println("entry = " + entry)
      acc + entry
    }

  def main(args: Array[String]) {
    val logFile = "project3" // Should be some file on your system
    //val logFile = "oneLine"
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile).take(1000) //,2).cache()
    //val logData = sc.textFile(logFile)


    //val documents = logData.flatMap(line => line.split(" ")).filter(line => line.contains("gene_")).map(line => (line, 1)).reduceByKey(_ + _)
    val documents = logData.flatMap(line => line.split("\t"))

    //val rdd = logData.flatMap(line => line.split(" ").filter(line => line.contains("gene_")).map(word => (word, 1)).reduce(_ + _))



    //val words = documents.foreach(line => line.split(" ").filter(line => line.contains("gene_")).distinct
//    val yourRdd = documents.map(arr => {
//      val words = arr.split( " " ).filter(line => line.contains("gene_")).distinct
//      words.map( word => ( word, 1 ) )
//    } )

    // Initialize arraybuffer for gene terms
    var geneArray = ArrayBuffer[(String, Int)]()

    documents.foreach(a => {
      // Separate word for each document, only choose gene term without duplicates in single document
      val words = a.split(" ").filter(line => line.contains("gene_")).map(line => (line, 1)).distinct

      // For each document, we append all gene term into a big array for mapReduce functions later
      for ( i <- words){
        geneArray += i
      }
    })

    // Create RDDs: parallelizing collection
    val distData = sc.parallelize(geneArray)

    // Map all duplicates
    val reducedTotalGene = distData.reduceByKey(_ + _)

    //reducedTotalGene.saveAsTextFile("result")
    reducedTotalGene.foreach( a => {
      println(a)
    })

    // Now, if you want to print this...
    //found.foreach( { case ( word, title ) => mapForGeneTerms } )
    //words.foreach( println )

//    val numOfDocuments = logData.count()
//    val numOfGenes = logData.filter(line => line.contains("gene_")).count()
//    println("Lines with a: %s, Lines with b: %s".format(numOfDocuments, numOfGenes))
  }
}
