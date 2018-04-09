// Databricks notebook source
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

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
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


// COMMAND ----------

import spark.implicits._
 spark.version                                    
spark.catalog.currentDatabase                     //> res1: String = default
spark.catalog.listDatabases.show()               

val fn = "/FileStore/tables/breastCancer.csv"
                                                  //> fn  : String = c:/aaprograms/datasets/BreastCancer/breastCancer.csv


// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
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
                    .add("clas","integer") 

// COMMAND ----------

val dfBreast = spark.read.format("csv").option("header", false).schema(recordSchema).load(fn)
              
                                    

// COMMAND ----------

dfBreast.createOrReplaceTempView("BreastCancer")
 val sqlDF = spark.sql("Select clas from BreastCancer")
                                                  
spark.catalog.currentDatabase                     
spark.catalog.listDatabases.show()  

// COMMAND ----------

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
         }

// COMMAND ----------

cancerRDD.partitions.size
val cancerDS = cancerRDD.toDS() 

// COMMAND ----------

val bin = (i: Int) => if (i == 4) 1 else 0  

// COMMAND ----------

  spark.udf.register("udfBinarizeClas", (s: Int) => bin(s))

// COMMAND ----------

val sqlUDF = spark.sql("Select * , udfBinarizeClas(clas) from BreastCancer")

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler 
 sqlUDF.take(10).foreach(println)
val assembler = new VectorAssembler().setInputCols(Array("sECSize","cThick"))
                                     .setOutputCol("features")

// COMMAND ----------

val output = assembler.transform(sqlUDF)

// COMMAND ----------

output.show()

// COMMAND ----------

val mlready = output.withColumnRenamed("UDF:udfBinarizeClas(clas)", "label")

// COMMAND ----------

mlready.show()

// COMMAND ----------

import org.apache.spark.ml.classification.LogisticRegression
val lr = new LogisticRegression()
val lrModel = lr.fit(mlready)

// COMMAND ----------

val coefficients = lrModel.coefficients



val intercept = lrModel.intercept

// COMMAND ----------


