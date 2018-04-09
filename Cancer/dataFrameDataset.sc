/** project: IFT443Spring2018
ws: eclipsewsOld
sc: dataFrameDataset
Reading the Sarkar txt,(breast Cancer classification ) working thru his examples

2018-02-10
-------------------
Given the data set URL that we have used in class: This data is at UCI machine learning repository....

https://archive.ics.uci.edu/ml/datasets/Breast+Cancer+Wisconsin+(Diagnostic)

(here is that breast cancer data file):

breastCancer.csv
 
There are only 2 phases here : Using  Logistic Regression

A. Do a classification using just ONE feature and get a  linear classifier as we did in our Hosmer data,  there, we used just Age.
Here, it is not clear what
feature is best, so, just pick one...( can you think of a way to determine a 'good'  feature ( correlation maybe)?

B. Do a classification using just TWO features

For each of A and B, do the following..

1. read the data set

2. parse it in to a prepared case class

3. restructure it to produce a dataframe in the proper format for ML

4. Use  a logistic regression model to determine the contribution of the feaure(s)..

5. How can you interpret your resutls??  See the Hosmer paper....

Here is some code to get you started

dataFrameDataset.sc

*/

package apps
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, explode, length, split, substring}
import org.apache.spark.sql.types._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier, DecisionTreeClassificationModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

//case class
case class CancerClass(sample : Long,
                       cThick : Int,
                       uCSize: Int,
                       UCShape: Int,
                       mAdhes : Int,
                       sECSize : Int,
                          bNuc : Int,
                         bChrom: Int,
                           nNuc: Int,
                         mitosis: Int,
                            clas: Int)

   
object dataFrameDataset {
 Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder
             .master("local[*]")
             .appName("IFT443Spring2018")
             .getOrCreate()                       //> Using Spark's default log4j profile: org/apache/spark/log4j-defaults.proper
                                                  //| ties
                                                  //| SLF4J: Class path contains multiple SLF4J bindings.
                                                  //| SLF4J: Found binding in [jar:file:/C:/Users/Pramod%20Mekapothula/.ivy2/cach
                                                  //| e/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-1.7.16.jar!/org/slf4j/impl/Sta
                                                  //| ticLoggerBinder.class]
                                                  //| SLF4J: Found binding in [jar:file:/C:/Users/Pramod%20Mekapothula/.ivy2/cach
                                                  //| e/org.slf4j/slf4j-simple/jars/slf4j-simple-1.7.5.jar!/org/slf4j/impl/Static
                                                  //| LoggerBinder.class]
                                                  //| SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explana
                                                  //| tion.
                                                  //| SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
                                                  //| spark  : org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSess
                                                  //| ion@10ad20cb
 import spark.implicits._
 spark.version                                    //> res0: String = 2.1.0
spark.catalog.currentDatabase                     //> res1: String = default
spark.catalog.listDatabases.show()                //> +-------+----------------+--------------------+
                                                  //| |   name|     description|         locationUri|
                                                  //| +-------+----------------+--------------------+
                                                  //| |default|default database|file:/C:/pm%20dat...|
                                                  //| +-------+----------------+--------------------+
                                                  //| 

val fn = "c:/ScalaFiles/breastCancer.csv"         //> fn  : String = c:/ScalaFiles/breastCancer.csv
// "add" is a method on the StructType object
val recordSchema = new StructType()
                    .add("sample","long")
                    .add("cThick","integer")
                    .add("uCSize","integer")
                    .add("uCShape","integer")
                    .add("mAdhes","integer")
                    .add("sECSize","integer")
                    .add("bNuc","integer")
                    .add("bChrom","integer")
                    .add("nNuc","integer")
                    .add("mitosis","integer")
                    .add("clas","integer")        //> recordSchema  : org.apache.spark.sql.types.StructType = StructType(StructFi
                                                  //| eld(sample,LongType,true), StructField(cThick,IntegerType,true), StructFiel
                                                  //| d(uCSize,IntegerType,true), StructField(uCShape,IntegerType,true), StructFi
                                                  //| eld(mAdhes,IntegerType,true), StructField(sECSize,IntegerType,true), Struct
                                                  //| Field(bNuc,IntegerType,true), StructField(bChrom,IntegerType,true), StructF
                                                  //| ield(nNuc,IntegerType,true), StructField(mitosis,IntegerType,true), StructF
                                                  //| ield(clas,IntegerType,true))
                    
val dfBreast = spark.read.format("csv").option("header", false).schema(recordSchema).load(fn)
                                                  //> dfBreast  : org.apache.spark.sql.DataFrame = [sample: bigint, cThick: int .
                                                  //| .. 9 more fields]
 //dfBreast.show()
 dfBreast.createOrReplaceTempView("BreastCancer")
 val sqlDF = spark.sql("Select clas from BreastCancer")
                                                  //> sqlDF  : org.apache.spark.sql.DataFrame = [clas: int]
spark.catalog.currentDatabase                     //> res2: String = default
spark.catalog.listDatabases.show()                //> +-------+----------------+--------------------+
                                                  //| |   name|     description|         locationUri|
                                                  //| +-------+----------------+--------------------+
                                                  //| |default|default database|file:/C:/pm%20dat...|
                                                  //| +-------+----------------+--------------------+
                                                  //| 
  
 //sqlDF.take(5).foreach(println)
 //now create a Dataset using the case class "CancerClass" with 4 partitions
 val cancerRDD = spark.sparkContext.textFile(fn, 4)
     .map{ line=> {
       val ar = line.split(",")
       CancerClass(ar(0).trim.toLong,
                   ar(1).trim.toInt,
                   ar(2).trim.toInt,
                   ar(3).trim.toInt,
                   ar(4).trim.toInt,
                   ar(5).trim.toInt,
                   ar(6).trim.toInt,
                   ar(7).trim.toInt,
                   ar(8).trim.toInt,
                   ar(9).trim.toInt,
                   ar(10).trim.toInt)
                   
                  }
         }//end cancerRDD                         //> cancerRDD  : org.apache.spark.rdd.RDD[apps.CancerClass] = MapPartitionsRDD[
                                                  //| 3] at map at apps.dataFrameDataset.scala:110
    cancerRDD.partitions.size                     //> res3: Int = 4
   val cancerDS = cancerRDD.toDS()                //> cancerDS  : org.apache.spark.sql.Dataset[apps.CancerClass] = [sample: bigin
                                                  //| t, cThick: int ... 9 more fields]
                                                  
   //**** For some reason, defining a function causes an abort!! but
   // using a val does work., the def bin below blows up while the val is ok???
   
   //*** NO GOOD   def binx(s: Int): Int = s match { case 2 => 0 case 4 => 1}
  //OK but can be edited to be shorter as below val bin: Int => Int = (i: Int) => if (i == 4) 1 else 0
    val bin = (i: Int) => if (i == 4) 1 else 0    //> bin  : Int => Int = <function1>
  
 
   //spark.udf.register("udfBinarizeClas", (s: Int) => s match {case 2 => 0 case 4 => 1 } )
  spark.udf.register("udfBinarizeClas", (s: Int) => bin(s))
                                                  //> res4: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFun
                                                  //| ction(<function1>,IntegerType,Some(List(IntegerType)))
   
  
   val sqlUDF = spark.sql("Select * , udfBinarizeClas(clas) from BreastCancer")
                                                  //> sqlUDF  : org.apache.spark.sql.DataFrame = [sample: bigint, cThick: int ...
                                                  //|  10 more fields]
   sqlUDF.take(10).foreach(println)               //> [1000025,5,1,1,1,2,1,3,1,1,2,0]
                                                  //| [1002945,5,4,4,5,7,10,3,2,1,2,0]
                                                  //| [1015425,3,1,1,1,2,2,3,1,1,2,0]
                                                  //| [1016277,6,8,8,1,3,4,3,7,1,2,0]
                                                  //| [1017023,4,1,1,3,2,1,3,1,1,2,0]
                                                  //| [1017122,8,10,10,8,7,10,9,7,1,4,1]
                                                  //| [1018099,1,1,1,1,2,10,3,1,1,2,0]
                                                  //| [1018561,2,1,2,1,2,1,3,1,1,2,0]
                                                  //| [1033078,2,1,1,1,2,1,1,1,5,2,0]
                                                  //| [1033078,4,2,1,1,2,1,2,1,1,2,0]
   
  //displaying DataFrame
  sqlUDF.show()                                   //> +-------+------+------+-------+------+-------+----+------+----+-------+----
                                                  //| +---------+
                                                  //| | sample|cThick|uCSize|uCShape|mAdhes|sECSize|bNuc|bChrom|nNuc|mitosis|clas
                                                  //| |UDF(clas)|
                                                  //| +-------+------+------+-------+------+-------+----+------+----+-------+----
                                                  //| +---------+
                                                  //| |1000025|     5|     1|      1|     1|      2|   1|     3|   1|      1|   2
                                                  //| |        0|
                                                  //| |1002945|     5|     4|      4|     5|      7|  10|     3|   2|      1|   2
                                                  //| |        0|
                                                  //| |1015425|     3|     1|      1|     1|      2|   2|     3|   1|      1|   2
                                                  //| |        0|
                                                  //| |1016277|     6|     8|      8|     1|      3|   4|     3|   7|      1|   2
                                                  //| |        0|
                                                  //| |1017023|     4|     1|      1|     3|      2|   1|     3|   1|      1|   2
                                                  //| |        0|
                                                  //| |1017122|     8|    10|     10|     8|      7|  10|     9|   7|      1|   4
                                                  //| |        1|
                                                  //| |1018099|     1|     1|      1|     1|      2|  10|     3|   1|      1|   2
                                                  //| |        0|
                                                  //| |1018561|     2
                                                  //| Output exceeds cutoff limit.
 
 //Renaming clas as "label" for ML
 val typedDS = sqlUDF.select($"UDF(clas)".cast(IntegerType).as("label"), $"cThick",$"uCSize", $"uCShape", $"mAdhes",
 																									$"sECSize", $"bNuc", $"bChrom", $"nNuc", $"mitosis")
                                                  //> typedDS  : org.apache.spark.sql.DataFrame = [label: int, cThick: int ... 8 
                                                  //| more fields]

typedDS.show()                                    //> +-----+------+------+-------+------+-------+----+------+----+-------+
                                                  //| |label|cThick|uCSize|uCShape|mAdhes|sECSize|bNuc|bChrom|nNuc|mitosis|
                                                  //| +-----+------+------+-------+------+-------+----+------+----+-------+
                                                  //| |    0|     5|     1|      1|     1|      2|   1|     3|   1|      1|
                                                  //| |    0|     5|     4|      4|     5|      7|  10|     3|   2|      1|
                                                  //| |    0|     3|     1|      1|     1|      2|   2|     3|   1|      1|
                                                  //| |    0|     6|     8|      8|     1|      3|   4|     3|   7|      1|
                                                  //| |    0|     4|     1|      1|     3|      2|   1|     3|   1|      1|
                                                  //| |    1|     8|    10|     10|     8|      7|  10|     9|   7|      1|
                                                  //| |    0|     1|     1|      1|     1|      2|  10|     3|   1|      1|
                                                  //| |    0|     2|     1|      2|     1|      2|   1|     3|   1|      1|
                                                  //| |    0|     2|     1|      1|     1|      2|   1|     1|   1|      5|
                                                  //| |    0|     4|     2|      1|     1|      2|   1|     2|   1|      1|
                                                  //| |    0|     1|     1
                                                  //| Output exceeds cutoff limit.
  
 typedDS.dtypes                                   //> res5: Array[(String, String)] = Array((label,IntegerType), (cThick,IntegerT
                                                  //| ype), (uCSize,IntegerType), (uCShape,IntegerType), (mAdhes,IntegerType), (s
                                                  //| ECSize,IntegerType), (bNuc,IntegerType), (bChrom,IntegerType), (nNuc,Intege
                                                  //| rType), (mitosis,IntegerType))

/********* One feature sECSize ************/

 val assembler1 = new VectorAssembler()
 											.setInputCols(Array("sECSize"))
 											.setOutputCol("features")
                                                  //> assembler1  : org.apache.spark.ml.feature.VectorAssembler = vecAssembler_02
                                                  //| ae96f98b9e
 
 val outputDS1 = assembler1.transform(typedDS)    //> outputDS1  : org.apache.spark.sql.DataFrame = [label: int, cThick: int ... 
                                                  //| 9 more fields]
  
 outputDS1.show()                                 //> +-----+------+------+-------+------+-------+----+------+----+-------+------
                                                  //| --+
                                                  //| |label|cThick|uCSize|uCShape|mAdhes|sECSize|bNuc|bChrom|nNuc|mitosis|featur
                                                  //| es|
                                                  //| +-----+------+------+-------+------+-------+----+------+----+-------+------
                                                  //| --+
                                                  //| |    0|     5|     1|      1|     1|      2|   1|     3|   1|      1|   [2.
                                                  //| 0]|
                                                  //| |    0|     5|     4|      4|     5|      7|  10|     3|   2|      1|   [7.
                                                  //| 0]|
                                                  //| |    0|     3|     1|      1|     1|      2|   2|     3|   1|      1|   [2.
                                                  //| 0]|
                                                  //| |    0|     6|     8|      8|     1|      3|   4|     3|   7|      1|   [3.
                                                  //| 0]|
                                                  //| |    0|     4|     1|      1|     3|      2|   1|     3|   1|      1|   [2.
                                                  //| 0]|
                                                  //| |    1|     8|    10|     10|     8|      7|  10|     9|   7|      1|   [7.
                                                  //| 0]|
                                                  //| |    0|     1|     1|      1|     1|      2|  10|     3|   1|      1|   [2.
                                                  //| 0]|
                                                  //| |    0|     2|     1|      2|     1|      2|   1|     3|   1|      1|   [2.
                                                  //| 0]|
                                                  //| |    
                                                  //| Output exceeds cutoff limit.

outputDS1.dtypes                                  //> res6: Array[(String, String)] = Array((label,IntegerType), (cThick,IntegerT
                                                  //| ype), (uCSize,IntegerType), (uCShape,IntegerType), (mAdhes,IntegerType), (s
                                                  //| ECSize,IntegerType), (bNuc,IntegerType), (bChrom,IntegerType), (nNuc,Intege
                                                  //| rType), (mitosis,IntegerType), (features,org.apache.spark.ml.linalg.VectorU
                                                  //| DT@3bfc3ba7))


//total number of rows
 val totalcount = outputDS1.count()               //> totalcount  : Long = 699
 
//Splitting DF into Testdata, trainingdata
// val Array(trainingData, testData) = outputDS.randomSplit(Array(0.7, 0.3))
//trainingData.show()
//trainingData.dtypes

 val dt1 = new DecisionTreeClassifier()
 							.setLabelCol("label")
 							.setFeaturesCol("features")
                                                  //> dt1  : org.apache.spark.ml.classification.DecisionTreeClassifier = dtc_8238
                                                  //| 1aca8942
 							
 
 val pipeline1 = new Pipeline()
 										.setStages(Array(dt1))
                                                  //> pipeline1  : org.apache.spark.ml.Pipeline = pipeline_01b53d490063
 										
 val model1 = pipeline1.fit(outputDS1)            //> model1  : org.apache.spark.ml.PipelineModel = pipeline_01b53d490063
 
 val predictions1 = model1.transform(outputDS1)   //> predictions1  : org.apache.spark.sql.DataFrame = [label: int, cThick: int .
                                                  //| .. 12 more fields]
 
 predictions1.select("prediction", "label", "features").show()
                                                  //> +----------+-----+--------+
                                                  //| |prediction|label|features|
                                                  //| +----------+-----+--------+
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       1.0|    0|   [7.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       1.0|    0|   [3.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       1.0|    1|   [7.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       0.0|    0|   [1.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       0.0|    1|   [2.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       1.0|    1|   [7.0]|
                                                  //| |       1.0|    1|   [6.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| |       1.0|    1|   [4.0]|
                                                  //| |       0.0|    0|   [2.0]|
                                                  //| +----------+-----+--------+
                                                  //| only showing top 20 rows
                                                  //| 
 
 val onefeature = predictions1.filter{ $"label" !== $"prediction" }.count
                                                  //> onefeature  : Long = 73
  
  
 var treeModel1 = model1.stages(0).asInstanceOf[DecisionTreeClassificationModel]
                                                  //> treeModel1  : org.apache.spark.ml.classification.DecisionTreeClassification
                                                  //| Model = DecisionTreeClassificationModel (uid=dtc_82381aca8942) of depth 5 w
                                                  //| ith 17 nodes
 
 println(s"Learned Classification tree model \n ${treeModel1.toDebugString}")
                                                  //> Learned Classification tree model 
                                                  //|  DecisionTreeClassificationModel (uid=dtc_82381aca8942) of depth 5 with 17 
                                                  //| nodes
                                                  //|   If (feature 0 <= 2.0)
                                                  //|    If (feature 0 <= 1.0)
                                                  //|     Predict: 0.0
                                                  //|    Else (feature 0 > 1.0)
                                                  //|     Predict: 0.0
                                                  //|   Else (feature 0 > 2.0)
                                                  //|    If (feature 0 <= 3.0)
                                                  //|     Predict: 1.0
                                                  //|    Else (feature 0 > 3.0)
                                                  //|     If (feature 0 <= 8.0)
                                                  //|      If (feature 0 <= 5.0)
                                                  //|       If (feature 0 <= 4.0)
                                                  //|        Predict: 1.0
                                                  //|       Else (feature 0 > 4.0)
                                                  //|        Predict: 1.0
                                                  //|      Else (feature 0 > 5.0)
                                                  //|       If (feature 0 <= 6.0)
                                                  //|        Predict: 1.0
                                                  //|       Else (feature 0 > 6.0)
                                                  //|        Predict: 1.0
                                                  //|     Else (feature 0 > 8.0)
                                                  //|      If (feature 0 <= 9.0)
                                                  //|       Predict: 1.0
                                                  //|      Else (feature 0 > 9.0)
                                                  //|       Predict: 1.0
                                                  //| 

val accuracy1 =  100 - ((onefeature *100) / totalcount)
                                                  //> accuracy1  : Long = 90

/********* Two features uCSize, uCShape ***********/
 val assembler = new VectorAssembler()
 											.setInputCols(Array("uCSize", "uCShape"))
 											.setOutputCol("features")
                                                  //> assembler  : org.apache.spark.ml.feature.VectorAssembler = vecAssembler_497
                                                  //| 0a2158fc8
 
 val outputDS = assembler.transform(typedDS)      //> outputDS  : org.apache.spark.sql.DataFrame = [label: int, cThick: int ... 9
                                                  //|  more fields]
  
 outputDS.show()                                  //> +-----+------+------+-------+------+-------+----+------+----+-------+------
                                                  //| -----+
                                                  //| |label|cThick|uCSize|uCShape|mAdhes|sECSize|bNuc|bChrom|nNuc|mitosis|   fea
                                                  //| tures|
                                                  //| +-----+------+------+-------+------+-------+----+------+----+-------+------
                                                  //| -----+
                                                  //| |    0|     5|     1|      1|     1|      2|   1|     3|   1|      1|  [1.0
                                                  //| ,1.0]|
                                                  //| |    0|     5|     4|      4|     5|      7|  10|     3|   2|      1|  [4.0
                                                  //| ,4.0]|
                                                  //| |    0|     3|     1|      1|     1|      2|   2|     3|   1|      1|  [1.0
                                                  //| ,1.0]|
                                                  //| |    0|     6|     8|      8|     1|      3|   4|     3|   7|      1|  [8.0
                                                  //| ,8.0]|
                                                  //| |    0|     4|     1|      1|     3|      2|   1|     3|   1|      1|  [1.0
                                                  //| ,1.0]|
                                                  //| |    1|     8|    10|     10|     8|      7|  10|     9|   7|      1|[10.0,
                                                  //| 10.0]|
                                                  //| |    0|     1|     1|      1|     1|      2|  10|     3|   1|      1|  [1.0
                                                  //| ,1.0]|
                                                  //| |    0|     2|     1|      2|     1|      2|   1|     3|   1|    
                                                  //| Output exceeds cutoff limit.

outputDS.dtypes                                   //> res7: Array[(String, String)] = Array((label,IntegerType), (cThick,IntegerT
                                                  //| ype), (uCSize,IntegerType), (uCShape,IntegerType), (mAdhes,IntegerType), (s
                                                  //| ECSize,IntegerType), (bNuc,IntegerType), (bChrom,IntegerType), (nNuc,Intege
                                                  //| rType), (mitosis,IntegerType), (features,org.apache.spark.ml.linalg.VectorU
                                                  //| DT@3bfc3ba7))
 
//Splitting DF into Testdata, trainingdata
// val Array(trainingData, testData) = outputDS.randomSplit(Array(0.7, 0.3))
//trainingData.show()
//trainingData.dtypes


 val dt = new DecisionTreeClassifier()
 							.setLabelCol("label")
 							.setFeaturesCol("features")
                                                  //> dt  : org.apache.spark.ml.classification.DecisionTreeClassifier = dtc_aaecd
                                                  //| e834a0c
 							
 
 val pipeline = new Pipeline()
 										.setStages(Array(dt))
                                                  //> pipeline  : org.apache.spark.ml.Pipeline = pipeline_3bf35fbf0ee4
 										
 val model = pipeline.fit(outputDS)               //> model  : org.apache.spark.ml.PipelineModel = pipeline_3bf35fbf0ee4
 
 val predictions = model.transform(outputDS)      //> predictions  : org.apache.spark.sql.DataFrame = [label: int, cThick: int ..
                                                  //| . 12 more fields]
 
 predictions.select("prediction", "label", "features").show()
                                                  //> +----------+-----+-----------+
                                                  //| |prediction|label|   features|
                                                  //| +----------+-----+-----------+
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       1.0|    0|  [4.0,4.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       1.0|    0|  [8.0,8.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       1.0|    1|[10.0,10.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       0.0|    0|  [1.0,2.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       0.0|    0|  [2.0,1.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       1.0|    1|  [3.0,3.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       1.0|    1|  [7.0,5.0]|
                                                  //| |       1.0|    1|  [4.0,6.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| |       1.0|    1|  [7.0,7.0]|
                                                  //| |       0.0|    0|  [1.0,1.0]|
                                                  //| +----------+-----+-----------+
                                                  //| only showing top 20 rows
                                                  //| 
 
 val twofeatures = predictions.filter{ $"label" !== $"prediction" }.count
                                                  //> twofeatures  : Long = 38
  
  
 var treeModel = model.stages(0).asInstanceOf[DecisionTreeClassificationModel]
                                                  //> treeModel  : org.apache.spark.ml.classification.DecisionTreeClassificationM
                                                  //| odel = DecisionTreeClassificationModel (uid=dtc_aaecde834a0c) of depth 5 wi
                                                  //| th 37 nodes
 
 println(s"Learned Classification tree model \n ${treeModel.toDebugString}")
                                                  //> Learned Classification tree model 
                                                  //|  DecisionTreeClassificationModel (uid=dtc_aaecde834a0c) of depth 5 with 37 
                                                  //| nodes
                                                  //|   If (feature 0 <= 2.0)
                                                  //|    If (feature 1 <= 2.0)
                                                  //|     If (feature 1 <= 1.0)
                                                  //|      If (feature 0 <= 1.0)
                                                  //|       Predict: 0.0
                                                  //|      Else (feature 0 > 1.0)
                                                  //|       Predict: 0.0
                                                  //|     Else (feature 1 > 1.0)
                                                  //|      If (feature 0 <= 1.0)
                                                  //|       Predict: 0.0
                                                  //|      Else (feature 0 > 1.0)
                                                  //|       Predict: 0.0
                                                  //|    Else (feature 1 > 2.0)
                                                  //|     If (feature 0 <= 1.0)
                                                  //|      If (feature 1 <= 3.0)
                                                  //|       Predict: 0.0
                                                  //|      Else (feature 1 > 3.0)
                                                  //|       Predict: 0.0
                                                  //|     Else (feature 0 > 1.0)
                                                  //|      If (feature 1 <= 3.0)
                                                  //|       Predict: 0.0
                                                  //|      Else (feature 1 > 3.0)
                                                  //|       Predict: 1.0
                                                  //|   Else (feature 0 > 2.0)
                                                  //|    If (feature 1 <= 2.0)
                                                  //|     If (feature 1 <= 1.0)
                                                  //|      Predict: 0.0
                                                  //|     Else (feature 1 > 1.0)
                                                  //|      If (feature 0 <= 4.0)
                                                  //|       If (feature
                                                  //| Output exceeds cutoff limit.

  
  val accuracy =  100 - ((twofeatures *100) / totalcount)
                                                  //> accuracy  : Long = 95
  
 /********** 4 features ************/
 /********* four features mAdhes, bChrom, cThick, mitosis ***********/
 
 val assembler2 = new VectorAssembler()
 											.setInputCols(Array("cThick", "mAdhes", "bChrom", "mitosis","sECSize"))
 											.setOutputCol("features")
                                                  //> assembler2  : org.apache.spark.ml.feature.VectorAssembler = vecAssembler_90
                                                  //| 26b16b609d
 
 val outputDS2 = assembler2.transform(typedDS)    //> outputDS2  : org.apache.spark.sql.DataFrame = [label: int, cThick: int ... 
                                                  //| 9 more fields]
  
 outputDS2.show()                                 //> +-----+------+------+-------+------+-------+----+------+----+-------+------
                                                  //| --------------+
                                                  //| |label|cThick|uCSize|uCShape|mAdhes|sECSize|bNuc|bChrom|nNuc|mitosis|      
                                                  //|       features|
                                                  //| +-----+------+------+-------+------+-------+----+------+----+-------+------
                                                  //| --------------+
                                                  //| |    0|     5|     1|      1|     1|      2|   1|     3|   1|      1|[5.0,1
                                                  //| .0,3.0,1.0,...|
                                                  //| |    0|     5|     4|      4|     5|      7|  10|     3|   2|      1|[5.0,5
                                                  //| .0,3.0,1.0,...|
                                                  //| |    0|     3|     1|      1|     1|      2|   2|     3|   1|      1|[3.0,1
                                                  //| .0,3.0,1.0,...|
                                                  //| |    0|     6|     8|      8|     1|      3|   4|     3|   7|      1|[6.0,1
                                                  //| .0,3.0,1.0,...|
                                                  //| |    0|     4|     1|      1|     3|      2|   1|     3|   1|      1|[4.0,3
                                                  //| .0,3.0,1.0,...|
                                                  //| |    1|     8|    10|     10|     8|      7|  10|     9|   7|      1|[8.0,8
                                                  //| .0,9.0,1.0,...|
                                                  //| |    0|     1|     1|      1|     1|      2|  10|     3|   1|      1|[1.0,1
 outputDS2.dtypes                                 //> res8: Array[(String, String)] = Array((label,IntegerType), (cThick,IntegerT
                                                  //| ype), (uCSize,IntegerType), (uCShape,IntegerType), (mAdhes,IntegerType), (s
                                                  //| ECSize,IntegerType), (bNuc,IntegerType), (bChrom,IntegerType), (nNuc,Intege
                                                  //| rType), (mitosis,IntegerType), (features,org.apache.spark.ml.linalg.VectorU
                                                  //| DT@3bfc3ba7))
 
//Splitting DF into Testdata, trainingdata
// val Array(trainingData, testData) = outputDS.randomSplit(Array(0.7, 0.3))
//trainingData.show()
//trainingData.dtypes


 val dt2 = new DecisionTreeClassifier()
 							.setLabelCol("label")
 							.setFeaturesCol("features")
                                                  //> dt2  : org.apache.spark.ml.classification.DecisionTreeClassifier = dtc_b969
                                                  //| de56b5c5
 							
 
 val pipeline2 = new Pipeline()
 										.setStages(Array(dt2))
                                                  //> pipeline2  : org.apache.spark.ml.Pipeline = pipeline_20e104ecdd1d
 										
 val model2 = pipeline.fit(outputDS2)             //> model2  : org.apache.spark.ml.PipelineModel = pipeline_3bf35fbf0ee4
 
 val predictions2 = model.transform(outputDS2)    //> predictions2  : org.apache.spark.sql.DataFrame = [label: int, cThick: int .
                                                  //| .. 12 more fields]
 
 predictions2.select("prediction", "label", "features").show()
                                                  //> +----------+-----+--------------------+
                                                  //| |prediction|label|            features|
                                                  //| +----------+-----+--------------------+
                                                  //| |       0.0|    0|[5.0,1.0,3.0,1.0,...|
                                                  //| |       1.0|    0|[5.0,5.0,3.0,1.0,...|
                                                  //| |       0.0|    0|[3.0,1.0,3.0,1.0,...|
                                                  //| |       0.0|    0|[6.0,1.0,3.0,1.0,...|
                                                  //| |       1.0|    0|[4.0,3.0,3.0,1.0,...|
                                                  //| |       1.0|    1|[8.0,8.0,9.0,1.0,...|
                                                  //| |       0.0|    0|[1.0,1.0,3.0,1.0,...|
                                                  //| |       0.0|    0|[2.0,1.0,3.0,1.0,...|
                                                  //| |       0.0|    0|[2.0,1.0,1.0,5.0,...|
                                                  //| |       0.0|    0|[4.0,1.0,2.0,1.0,...|
                                                  //| |       0.0|    0|[1.0,1.0,3.0,1.0,...|
                                                  //| |       0.0|    0|[2.0,1.0,2.0,1.0,...|
                                                  //| |       1.0|    1|[5.0,3.0,4.0,1.0,...|
                                                  //| |       0.0|    0|[1.0,1.0,3.0,1.0,...|
                                                  //| |       1.0|    1|[8.0,10.0,5.0,4.0...|
                                                  //| |       1.0|    1|[7.0,4.0,4.0,1.0,...|
                                                  //| |       0.0|    0|[4.0,1.0,2.0,1.0,...|
                                                  //| |       0.0|    0|[4.0,1.0,3.0,1.0,...|
                                                  //| |       1.0|    1|[10.0,6.0,4.0,2.0...|
                                                  //| |    
                                                  //| Output exceeds cutoff limit.
 
 val allfeatures = predictions.filter{ $"label" !== $"prediction" }.count
                                                  //> allfeatures  : Long = 38
  
  
 var treeModel2 = model2.stages(0).asInstanceOf[DecisionTreeClassificationModel]
                                                  //> treeModel2  : org.apache.spark.ml.classification.DecisionTreeClassification
                                                  //| Model = DecisionTreeClassificationModel (uid=dtc_aaecde834a0c) of depth 5 w
                                                  //| ith 45 nodes
 
 println(s"Learned Classification tree model \n ${treeModel2.toDebugString}")
                                                  //> Learned Classification tree model 
                                                  //|  DecisionTreeClassificationModel (uid=dtc_aaecde834a0c) of depth 5 with 45 
                                                  //| nodes
                                                  //|   If (feature 2 <= 3.0)
                                                  //|    If (feature 0 <= 6.0)
                                                  //|     If (feature 1 <= 3.0)
                                                  //|      If (feature 4 <= 3.0)
                                                  //|       If (feature 3 <= 2.0)
                                                  //|        Predict: 0.0
                                                  //|       Else (feature 3 > 2.0)
                                                  //|        Predict: 0.0
                                                  //|      Else (feature 4 > 3.0)
                                                  //|       If (feature 1 <= 1.0)
                                                  //|        Predict: 0.0
                                                  //|       Else (feature 1 > 1.0)
                                                  //|        Predict: 1.0
                                                  //|     Else (feature 1 > 3.0)
                                                  //|      If (feature 2 <= 2.0)
                                                  //|       If (feature 0 <= 2.0)
                                                  //|        Predict: 1.0
                                                  //|       Else (feature 0 > 2.0)
                                                  //|        Predict: 0.0
                                                  //|      Else (feature 2 > 2.0)
                                                  //|       If (feature 1 <= 6.0)
                                                  //|        Predict: 0.0
                                                  //|       Else (feature 1 > 6.0)
                                                  //|        Predict: 1.0
                                                  //|    Else (feature 0 > 6.0)
                                                  //|     If (feature 4 <= 2.0)
                                                  //|      If (feature 0 <= 8.0)
                                                  //|       If (feature 2 <= 2.0)
                                                  //|        Predict: 
                                                  //| Output exceeds cutoff limit.

  
  val accuracy2 =  100 - ((allfeatures *100) / totalcount)
                                                  //> accuracy2  : Long = 95

}//end sc.dataFrameDataset