/** QuizWk10 Linear regression with Dummy variable
project: Test
ws: eclipsewsOld
sc: test1
The plan is to take some coursera data  to predict delivery times,given training
data on :
delivery times (minutes ) the target variable, 
0. minutes( Double
that is dependent on the following features
1.number of parcels delivered (Int
2. age of delivery truck (Int
3. region to be delivered to. ( This was coded as  region A , region B ( I re-coded 
to 1/0
The coefficients  are calculated to be:
minutes = intercept + 106 regionA + 10 parcels + 3.3 ageTruck
regionA  106. -> This means that deliveries to region A are 106 minutes longer than deliveries to B
parcels  10.  -> Every parcel adds 10 minutes to delivery time
age truck 3.3 -> every year of age of truck, add 3 minutes to delivery time
the intercept is -32,->  but is not significant  
btw: the results here match the Coursera calcs. 
rr. 2018-03-19
*/

package main.scala.apps
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, explode, length, split, substring}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.linalg.{ Vectors, Vector}


import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.ml.regression.{LinearRegression}
import org.apache.spark.ml.classification.LogisticRegression

import scala.io._

object test1 {
  println("QuizWk10 (Coursera data(edited)) Regional Dummy variable regression ")
                                                  //> QuizWk10 (COursera data) Regional Dummy variable regression 
  Logger.getLogger("org").setLevel(Level.OFF)
 val spark = SparkSession.builder
             .master("local[*]")
             .appName("IFT333FallNewBld")
             .getOrCreate()                       //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.proper
                                                  //| ties
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSess
                                                  //| ion@3e5d4f6b
 import spark.implicits._
 println(s" Spark version , ${spark.version} ")   //>  Spark version , 2.2.0 
 val sc = spark.sparkContext                      //> sc  : org.apache.spark.SparkContext = org.apache.spark.SparkContext@3d6a6be
                                                  //| e
val fn = "C:/aaprograms/datasets/DummyData/RegionParcelsTimeDummyCoursera.txt"
                                                  //> fn  : String = C:/aaprograms/datasets/DummyData/RegionParcelsTimeDummyCours
                                                  //| era.txt
val lines = sc.textFile(fn)                       //> lines  : org.apache.spark.rdd.RDD[String] = C:/aaprograms/datasets/DummyDat
                                                  //| a/RegionParcelsTimeDummyCoursera.txt MapPartitionsRDD[1] at textFile at mai
                                                  //| n.scala.apps.test1.scala:37
// the input file is (region, parcels, ageTruck, minutes)
// for the algorithm, I re-arranged them to
//minutes, parcels, ageTruck, region), then made the minutes -> label, and the other three a feature vector
val cleanLines = lines.filter(x => x!= "")        //> cleanLines  : org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[2] at fil
                                                  //| ter at main.scala.apps.test1.scala:41
val regressRDD = cleanLines.map{line =>
     val ar = line.split("""\s+""")
     val minutes = ar(3).trim.toDouble
     val parcels = ar(1).trim.toInt
     val ageTruck = ar(2).trim.toInt
     val region = if(ar(0) == "A")1 else 0
     Regress(minutes,parcels,ageTruck, region)
    }                                             //> regressRDD  : org.apache.spark.rdd.RDD[main.scala.apps.Regress] = MapPartit
                                                  //| ionsRDD[3] at map at main.scala.apps.test1.scala:42
    val regressDF = regressRDD.toDF("label", "parcels","ageTruck","region")
                                                  //> regressDF  : org.apache.spark.sql.DataFrame = [label: double, parcels: int 
                                                  //| ... 2 more fields]
    regressDF.show                                //> +-----+-------+--------+------+
                                                  //| |label|parcels|ageTruck|region|
                                                  //| +-----+-------+--------+------+
                                                  //| |489.4|     42|       3|     0|
                                                  //| |461.9|     46|      11|     0|
                                                  //| |447.9|     34|       9|     1|
                                                  //| |506.0|     44|       1|     1|
                                                  //| |303.0|     30|      13|     0|
                                                  //| |546.3|     48|       2|     1|
                                                  //| |273.0|     32|       1|     0|
                                                  //| |419.0|     44|       7|     0|
                                                  //| |486.2|     40|       4|     1|
                                                  //| |367.7|     36|      15|     0|
                                                  //| |380.6|     42|       2|     0|
                                                  //| |335.5|     38|       1|     0|
                                                  //| |264.9|     26|      14|     0|
                                                  //| |347.0|     24|      11|     1|
                                                  //| |321.3|     22|      12|     1|
                                                  //| |381.4|     26|      14|     1|
                                                  //| |226.7|     20|      15|     0|
                                                  //| |488.9|     36|       6|     1|
                                                  //| |474.4|     48|      10|     0|
                                                  //| |464.4|     36|       8|     1|
                                                  //| +-----+-------+--------+------+
                                                  //| only showing top 20 rows
                                                  //| 
    val assembler = new VectorAssembler()
            .setInputCols(Array("parcels","ageTruck","region"))
            .setOutputCol("features")             //> assembler  : org.apache.spark.ml.feature.VectorAssembler = vecAssembler_9eb
                                                  //| 661426bcf
     val inputFormat = assembler.transform(regressDF)
                                                  //> inputFormat  : org.apache.spark.sql.DataFrame = [label: double, parcels: in
                                                  //| t ... 3 more fields]
     val linReg = new LinearRegression()          //> linReg  : org.apache.spark.ml.regression.LinearRegression = linReg_5d01e1b6
                                                  //| 5e0c
     val linRegModel = linReg.fit(inputFormat)    //> 

   println(s"Coefficients: ${linRegModel.coefficients} intercept: ${linRegModel.intercept}")
                                                  //> Coefficients: [10.004923578947967,3.2643683389164746,106.13497262060649] in
                                                  //| tercept: -32.360672057943106
     
 
}//end
case class Regress(minutes: Double, region: Int, parcels: Int, ageTruck: Int)