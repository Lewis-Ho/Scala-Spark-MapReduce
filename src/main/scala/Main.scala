import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


/**
 * Created by yiucheungho on 5/9/15.
 */

object Main {
  def main(args: Array[String]) {
    val logFile = "project3" // Should be some file on your system
    //val logFile = "oneLine"
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile).take(9999999) //,2).cache()

    val logDataWh = sc.textFile(logFile)
    val lineCount = logDataWh.count()
    println("OUTPUT LINE COUNT " + lineCount)

    // Initialize arraybuffer for gene terms
    var geneArray = ArrayBuffer[(String, Int)]()
    // Initialize arraybuffer for tfidf result for every gene term
    var geneTermFreqArray = ArrayBuffer[(String, BigDecimal)]()

    // Get rid of tab
    val documents = logData.flatMap(line => line.split("\t"))


    // For each element, get rid of duplicate and write to new array
    documents.foreach(a => {
      // Separate word for each document, only choose gene term without duplicates in single document
      val words = a.split(" ").filter(line => line.contains("gene_")).map(line => (line, 1)).distinct

      // For each document, we append all gene term into a big array for mapReduce functions later
      for ( i <- words){
        geneArray += i
      }
    })

    val emptyRdd = sc.emptyRDD[(String, BigDecimal)]

    // Create RDDs: parallelizing collection, do calculation for each element
    val distData = sc.parallelize(geneArray).reduceByKey(_ + _).foreach( a=> {
      val a2 = BigDecimal.apply(a._2)
      val tf: BigDecimal = 1/a2
      val idf: BigDecimal = math.log(lineCount / a._2)
      val TFResult: BigDecimal = tf * idf
//      println(a + " " + a._1 +" " + a2 + " " + tf + " "  + idf + " " + TFResult)
//      println(" ")

      // Write to new array with term frequency result
      val tempTuple = (a._1, TFResult)
      //println(tempTuple)
      geneTermFreqArray += tempTuple
      println(geneTermFreqArray)
      // Create vector, for each gene term we get their similarity
//      sc.parallelize(geneTermFreqArray).reduceByKey(_ + _).foreach( a => {
//        sc.parallelize(geneTermFreqArray).reduceByKey(_ + _).foreach( b => {
//          //emptyRdd = a
//          // Same term, skip
//          if (a._1 == b._1){
//            // Skip
//          } else {
//            // Calculate Similarity
//            val similarity =
//          }
//        })
//      })
    })


    // Now, if you want to print this...
    //found.foreach( { case ( word, title ) => mapForGeneTerms } )

//    val numOfDocuments = logData.count()
//    val numOfGenes = logData.filter(line => line.contains("gene_")).count()
//    println("Lines with a: %s, Lines with b: %s".format(numOfDocuments, numOfGenes))
  }
}
