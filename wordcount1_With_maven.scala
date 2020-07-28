package com.intellij.maven.proj

// import org.apache.spark.storage.StorageLevel
// import org.apache.spark.sql.SparkSession

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


// This is best example of a maven project.
// in intellij for maven projects use Archetype as  - org.scala-tools.archetypes:scala-archetype-simple

object wordcount1_With_maven {
  def main(args: Array[String]): Unit = {
    val masterURL = args(0)

    val conf = new SparkConf().
      setAppName("wordcountWITHmavaen01").
      setMaster(masterURL)
    val sc = new SparkContext(conf)

    //you can create sc from sparkSession also like below
    /*
    val sc = SparkSession.builder
      .appName("wordcountWITHmavaen01")
      .master(masterURL)
      .getOrCreate()
      .sparkContext
    */
    val inputpath = args(1)
    //val outputpath = args(2)
    val wc = sc.textFile(inputpath).
      flatMap(line => line.split(" ")).
      map(word => (word, 1)).
      reduceByKey((acc, value) => acc + value)
    // .persist(StorageLevel.MEMORY_ONLY)

    //TO see HashPartitioner
    //val wc = sc.textFile(inputpath).
    //      flatMap(rec => rec.split(" ")).map(word => (word, 1)).partitionBy(new HashPartitioner(5))
    //val wc2 = wc.reduceByKey((acc, value) => acc + value)

    wc.take(100).foreach(println)
    //wc.saveAsTextFile(outputpath)
  }
}

/*
spark-submit --class com.intellij.maven.proj.wordcount1_With_maven
C:\Users\skothane\IdeaProjects\wordcount_With_maven\target\wordcount_With_maven-1.0-SNAPSHOT.jar
local D:\ETLHive\Testdata\Inputdata\word.txt
 */
