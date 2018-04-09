/** Manually calculating dummy regression using a Coursera dataset
project: Test
ws: eclipsewsOld
sc: manualRegressionRegionDummyVars.sc
I manually entered the Coursera data, illustrating dummy variables,
in a space delimited text data file ( the file has no headers)
Below is a regression using just the dummy variable, region.
The file has no header
col 1, region A or B needs to be converted to 1, 0
col 2, number of parcels delivered
col 3, age of delivery truck
col 4, minutes to delivery ( this is the target variable, to be predicted)
the first three columns are the features.

rr 2018-03-17
*/
package apps
import scala.io._


object manualRegressionRegionDummyVars {
type D = Double
type V = Vector[D]
def mean(v: V):D = v.sum/v.size                   //> mean: (v: apps.manualRegressionRegionDummyVars.V)apps.manualRegressionRegion
                                                  //| DummyVars.D
def dot(v:V, w: V): D = (v zip w).map{case(x,y) => x * y }.sum
                                                  //> dot: (v: apps.manualRegressionRegionDummyVars.V, w: apps.manualRegressionReg
                                                  //| ionDummyVars.V)apps.manualRegressionRegionDummyVars.D
def center(v : V) = v.map{ x => x - mean(v)}      //> center: (v: apps.manualRegressionRegionDummyVars.V)scala.collection.immutabl
                                                  //| e.Vector[Double]
val testv = Vector(1.0, 2.0, 3.0)                 //> testv  : scala.collection.immutable.Vector[Double] = Vector(1.0, 2.0, 3.0)
val testw = Vector(3.0,4, 5)                      //> testw  : scala.collection.immutable.Vector[Double] = Vector(3.0, 4.0, 5.0)
mean(testv)                                       //> res0: apps.manualRegressionRegionDummyVars.D = 2.0
dot(testv, testw)                                 //> res1: apps.manualRegressionRegionDummyVars.D = 26.0

//my local path
val fn = "C:\Users\LENOVO\Downloads\dummyTimesCategories"
                                                  //> fn  : String = C:/aaprograms/datasets/DummyData/RegionParcelsTimeDummyCours
                                                  //| era.txt
// there were empty lines, so I filtered them out
val lines = Source.fromFile(fn).getLines.toVector
            .filter(x => x!= "")                  //> lines  : scala.collection.immutable.Vector[String] = Vector(B 42 3 489.4, B
                                                  //|  46 11 461.9, A 34 9 447.9, A 44 1 506.0, B 30 13 303.0, A 48 2 546.3, B 32
                                                  //|  1 273.0, B 44 7 419.0, A 40 4 486.2, B 36 15 367.7, B 42 2 380.6, B 38 1 3
                                                  //| 35.5, B 26 14 264.9, A 24 11 347.0, A 22 12 321.3, A 26 14 381.4, B 20 15 2
                                                  //| 26.7, A 36 6 488.9, B 48 10 474.4, A 36 8 464.4, A 28 15 413.5, B 22 3 196.
                                                  //| 1, A 20 15 315.7, B 34 9 330.3, B 40 7 388.0, B 28 5 264.1, B 24 6 228.6, A
                                                  //|  32 9 434.0, A 46 1 517.9)

val minutes = lines.map{ line =>
    val ar = line.split("""\s+""")
    val convert = if (ar(0)== "A") 1 else 0
    Minutes(convert,ar(1).trim.toInt, ar(2).trim.toInt, ar(3).trim.toDouble)
    }                                             //> minutes  : scala.collection.immutable.Vector[apps.Minutes] = Vector(Minutes
                                                  //| (0,42,3,489.4), Minutes(0,46,11,461.9), Minutes(1,34,9,447.9), Minutes(1,44
                                                  //| ,1,506.0), Minutes(0,30,13,303.0), Minutes(1,48,2,546.3), Minutes(0,32,1,27
                                                  //| 3.0), Minutes(0,44,7,419.0), Minutes(1,40,4,486.2), Minutes(0,36,15,367.7),
                                                  //|  Minutes(0,42,2,380.6), Minutes(0,38,1,335.5), Minutes(0,26,14,264.9), Minu
                                                  //| tes(1,24,11,347.0), Minutes(1,22,12,321.3), Minutes(1,26,14,381.4), Minutes
                                                  //| (0,20,15,226.7), Minutes(1,36,6,488.9), Minutes(0,48,10,474.4), Minutes(1,3
                                                  //| 6,8,464.4), Minutes(1,28,15,413.5), Minutes(0,22,3,196.1), Minutes(1,20,15,
                                                  //| 315.7), Minutes(0,34,9,330.3), Minutes(0,40,7,388.0), Minutes(0,28,5,264.1)
                                                  //| , Minutes(0,24,6,228.6), Minutes(1,32,9,434.0), Minutes(1,46,1,517.9))
 // extract the target vector from the case class instances
val Y = for{ x <- minutes}
             yield x.minutes                      //> Y  : scala.collection.immutable.Vector[Double] = Vector(489.4, 461.9, 447.9
                                                  //| , 506.0, 303.0, 546.3, 273.0, 419.0, 486.2, 367.7, 380.6, 335.5, 264.9, 347
                                                  //| .0, 321.3, 381.4, 226.7, 488.9, 474.4, 464.4, 413.5, 196.1, 315.7, 330.3, 3
                                                  //| 88.0, 264.1, 228.6, 434.0, 517.9)
val y = center(Y)                                 //> y  : scala.collection.immutable.Vector[Double] = Vector(107.54827586206898,
                                                  //|  80.04827586206898, 66.04827586206898, 124.148275862069, -78.851724137931, 
                                                  //| 164.44827586206895, -108.851724137931, 37.148275862069, 104.34827586206899,
                                                  //|  -14.151724137931012, -1.251724137930978, -46.351724137931, -116.9517241379
                                                  //| 3102, -34.851724137931, -60.55172413793099, -0.4517241379310235, -155.15172
                                                  //| 4137931, 107.04827586206898, 92.54827586206898, 82.54827586206898, 31.64827
                                                  //| 5862069, -185.751724137931, -66.15172413793101, -51.55172413793099, 6.14827
                                                  //| 5862068999, -117.75172413793098, -153.251724137931, 52.148275862069, 136.04
                                                  //| 827586206898)
 // extract 'region from the case class instances
val X = for{ x <- region}
              yield x.region.toDouble             //> X  : scala.collection.immutable.Vector[Double] = Vector(0.0, 0.0, 1.0, 1.0,
                                                  //|  0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0,
                                                  //|  1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0)
val x = center(X)                                 //> x  : scala.collection.immutable.Vector[Double] = Vector(-0.4482758620689655
                                                  //| , -0.4482758620689655, 0.5517241379310345, 0.5517241379310345, -0.448275862
                                                  //| 0689655, 0.5517241379310345, -0.4482758620689655, -0.4482758620689655, 0.55
                                                  //| 17241379310345, -0.4482758620689655, -0.4482758620689655, -0.44827586206896
                                                  //| 55, -0.4482758620689655, 0.5517241379310345, 0.5517241379310345, 0.55172413
                                                  //| 79310345, -0.4482758620689655, 0.5517241379310345, -0.4482758620689655, 0.5
                                                  //| 517241379310345, 0.5517241379310345, -0.4482758620689655, 0.551724137931034
                                                  //| 5, -0.4482758620689655, -0.4482758620689655, -0.4482758620689655, -0.448275
                                                  //| 8620689655, 0.5517241379310345, 0.5517241379310345)
val b1 = dot(x,y)/dot(x,x)                        //> b1  : Double = 98.49230769230768
val a = mean(Y) - b1 * mean(X)                    //> a  : Double = 337.7
//minutes to deliver to region A is 98.49 more than delivery to region B, that is shown by b1
val yhat = a + b1 * 1                             //> yhat  : Double = 436.1923076923077




}//manualRegressionRegionDummyVars
case class Minutes(region: Int, parcels: Int, ageTruck: Int, minutes: Double)