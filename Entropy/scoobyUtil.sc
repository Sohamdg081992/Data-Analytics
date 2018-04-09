package main.scala.apps
import scala.io.Source
import scala.math._

object scoobyUtil {
	  println("Welcome to the Scala worksheet")
                                                  //> Welcome to the Scala worksheet
                                                  
    val loc = "C:/Users/LENOVO/Downloads/stickfigures.txt"
    
                                                  
	 
	                                                  
	 val figureData = Source.fromFile(loc).getLines.toList
                                                  //> figureData  : List[String] = List("1,1,1,1,	 ", "1,1,1,0, ", "1,1,1,1, ", "1
                                                  //| ,1,1,0, ", "1,1,1,1, ", "0,1,0,0, ", "0,0,0,0, ", "0,1,0,0, ", "0,1,0,0, ", 
                                                  //| "0,0,1,1, ", 1,0,0,1,, 1,0,0,0)
	 figureData.foreach(println)              //> 1,1,1,1,	 
                                                  //| 1,1,1,0, 
                                                  //| 1,1,1,1, 
                                                  //| 1,1,1,0, 
                                                  //| 1,1,1,1, 
                                                  //| 0,1,0,0, 
                                                  //| 0,0,0,0, 
                                                  //| 0,1,0,0, 
                                                  //| 0,1,0,0, 
                                                  //| 0,0,1,1, 
                                                  //| 1,0,0,1,
                                                  //| 1,0,0,0
	                                                 
	                                                  
	 val scoobyFiguresArrayStrings = figureData.map(rowString => rowString.split(","))
                                                  //> scoobyFiguresArrayStrings  : List[Array[String]] = List(Array(1, 1, 1, 1, "	
                                                  //|  "), Array(1, 1, 1, 0, " "), Array(1, 1, 1, 1, " "), Array(1, 1, 1, 0, " "),
                                                  //|  Array(1, 1, 1, 1, " "), Array(0, 1, 0, 0, " "), Array(0, 0, 0, 0, " "), Arr
                                                  //| ay(0, 1, 0, 0, " "), Array(0, 1, 0, 0, " "), Array(0, 0, 1, 1, " "), Array(1
                                                  //| , 0, 0, 1), Array(1, 0, 0, 0))
	                                                  
	 val stickFigures = scoobyFiguresArrayStrings. map(array => {
	     ScoobyFigure(array(0).trim.toInt, array(1).trim.toInt, array(2).trim.toInt, array(3).trim.toInt)
	      }
	     )                                    //> stickFigures  : List[main.scala.apps.ScoobyFigure] = List(ScoobyFigure(1,1,1
                                                  //| ,1), ScoobyFigure(1,1,1,0), ScoobyFigure(1,1,1,1), ScoobyFigure(1,1,1,0), Sc
                                                  //| oobyFigure(1,1,1,1), ScoobyFigure(0,1,0,0), ScoobyFigure(0,0,0,0), ScoobyFig
                                                  //| ure(0,1,0,0), ScoobyFigure(0,1,0,0), ScoobyFigure(0,0,1,1), ScoobyFigure(1,0
                                                  //| ,0,1), ScoobyFigure(1,0,0,0))
	 stickFigures.foreach(println)            //> ScoobyFigure(1,1,1,1)
                                                  //| ScoobyFigure(1,1,1,0)
                                                  //| ScoobyFigure(1,1,1,1)
                                                  //| ScoobyFigure(1,1,1,0)
                                                  //| ScoobyFigure(1,1,1,1)
                                                  //| ScoobyFigure(0,1,0,0)
                                                  //| ScoobyFigure(0,0,0,0)
                                                  //| ScoobyFigure(0,1,0,0)
                                                  //| ScoobyFigure(0,1,0,0)
                                                  //| ScoobyFigure(0,0,1,1)
                                                  //| ScoobyFigure(1,0,0,1)
                                                  //| ScoobyFigure(1,0,0,0)
 
                 
	  def ent( figs: List[ScoobyFigure], subListDiscriminator: String= "label"): (Double, Double, Double) =
	  {
			  	def ln2(d: Double) = log(d)/log(2.0)
			  def e( ones: Int, zeros: Int):Double =
			  {
			  		val total= (ones + zeros).toDouble
           val p = ones.toDouble/total
           val q = 1.0 - p
           if (p * q == 0) 0.0
           else  -( p * ln2(p) + q * ln2(q) )
			   }
		     val entropy:(Double,Double, Double) = subListDiscriminator match
		     {
				    case "label" =>{ val ones  = figs.count(fig => fig.label == 1)
				                     val zeros = figs.count(fig => fig.label == 0)
				                     println(s" $ones,  $zeros")
				                     val temp = e(ones,zeros)
				                      (temp, temp, temp )
				                     }
				    case "bodyType" => { val rectangleBods = figs.filter(fig => fig.bodyType == 1)
				                         val rectangleCount = rectangleBods.size
				                         val ones =  rectangleBods.count(fig => fig.label == 1)
				                         val zeros = rectangleBods.count(fig => fig.label == 0)
				                         val rectangleEntropy:Double = e(ones,zeros)
				                         val ovalBods = figs.filter(fig => fig.bodyType == 0 )
				                         val ovalBodsCount = ovalBods.size
				                         val onesx =  ovalBods.count(fig => fig.label == 1)
				                         val zerosx = ovalBods.count(fig => fig.label == 0)
				                         val ovalEntropy:Double = e(onesx,zerosx)
				                         val total = (rectangleCount + ovalBodsCount).toDouble
				                         (rectangleEntropy, ovalEntropy,
				                           rectangleCount/total * rectangleEntropy + ovalBodsCount/total * ovalEntropy )
				                
				                     }
				    case "headType" =>
				    						{
					    						  val squareBods = figs.filter(fig => fig.headType == 1)
	                         val squareCount = squareBods.size
	                         val ones =  squareBods.count(fig => fig.label == 1)
	                         val zeros = squareBods.count(fig => fig.label == 0)
	                         val squareEntropy:Double = e(ones,zeros)
	                         val circleBods = figs.filter(fig => fig.headType == 0 )
	                         val circleBodsCount = circleBods.size
	                         val onesx =  circleBods.count(fig => fig.label == 1)
	                         val zerosx = circleBods.count(fig => fig.label == 0)
	                         val circleEntropy:Double = e(onesx,zerosx)
	                         val total = (squareCount + circleBodsCount).toDouble
	                         (squareEntropy, circleEntropy,
	                          squareCount/total * squareEntropy + circleBodsCount/total * circleEntropy )/* code for this type*/
					    					}
				    case "colorType" =>
				    						{
	    										  val blackBods = figs.filter(fig => fig.bodyType == 1)
	                         val blackCount = blackBods.size
	                         val ones =  blackBods.count(fig => fig.label == 1)
	                         val zeros = blackBods.count(fig => fig.label == 0)
	                         val blackEntropy:Double = e(ones,zeros)
	                         val whiteBods = figs.filter(fig => fig.bodyType == 0 )
	                         val whiteCount = whiteBods.size
	                         val onesx =  whiteBods.count(fig => fig.label == 1)
	                         val zerosx = whiteBods.count(fig => fig.label == 0)
	                         val whiteEntropy:Double = e(onesx,zerosx)
	                         val total = (blackCount + whiteCount).toDouble
	                         (blackEntropy, whiteEntropy,
	                           blackCount/total * blackEntropy + whiteCount/total * whiteEntropy )
	    										}
	    				case "body-rectangle-color" =>
	    										{
	    											val rectangleBods = figs.filter(fig => fig.bodyType == 1)
	    											val blackRectBods = rectangleBods.filter( rB => rB.colorType == 1)
	    											val blackRectCount = blackRectBods.size
	    											val ones =  blackRectBods.count(fig => fig.label == 1)
	                          val zeros = blackRectBods.count(fig => fig.label == 0)
	                          val blackRectEntropy:Double = e(ones,zeros)
	                          val whiteRectBods = rectangleBods.filter( rB => rB.colorType == 0)
	                          val whiteRectCount = whiteRectBods.size
	    											val onesX =  whiteRectBods.count(fig => fig.label == 1)
	                          val zerosX = whiteRectBods.count(fig => fig.label == 0)
	                          val whiteRectEntropy:Double = e(onesX,zerosX)
	                          val total = (blackRectCount + whiteRectCount).toDouble
	                          (blackRectEntropy, whiteRectEntropy,
	                           blackRectCount/total * blackRectEntropy + whiteRectCount/total * whiteRectEntropy )
	    										}
	    										
	    					case "body-oval-head" =>
	    										{
	    											val ovalBods = figs.filter(fig => fig.bodyType == 0)
	    											val squareOvalBods = ovalBods.filter( oB => oB.headType == 1)
	    											val squareOvalCount = squareOvalBods.size
	    											val ones =  squareOvalBods.count(fig => fig.label == 1)
	                          val zeros = squareOvalBods.count(fig => fig.label == 0)
	                          val squareOvalEntropy:Double = e(ones,zeros)
	                         
	                          val circleOvalBods = ovalBods.filter( oB => oB.headType == 0)
	                          val circleOvalCount = circleOvalBods.size
	    											val onesX =  circleOvalBods.count(fig => fig.label == 1)
	                          val zerosX = circleOvalBods.count(fig => fig.label == 0)
	                          val circleOvalEntropy:Double = e(onesX,zerosX)
	                          val total = (squareOvalCount + circleOvalCount).toDouble
	                          (squareOvalEntropy, circleOvalEntropy,
	                           squareOvalCount/total * squareOvalEntropy + circleOvalCount/total * circleOvalEntropy )
	    										}
				    // case _ => is the default, 'no match'  and just returns dummy values
				      case _ => (42.0, 42.0, 42.0)
	   	  }//end match
			entropy
  }                                               //> ent: (figs: List[main.scala.apps.ScoobyFigure], subListDiscriminator: Strin
                                                  //| g)(Double, Double, Double)
  
  def informationGain(parentEntropy:Double, avgChildEntropy: Double) : Double =
  {
  		val ig = parentEntropy - avgChildEntropy;
  		ig
  }                                               //> informationGain: (parentEntropy: Double, avgChildEntropy: Double)Double
  
  val testXs = List(ScoobyFigure(1, 0, 1, 0), ScoobyFigure(1,0,1,1), ScoobyFigure(1, 0, 1, 0),
  ScoobyFigure(0,1,0,0), ScoobyFigure(0,1,1,0), ScoobyFigure(0,0,1,0))
                                                  //> testXs  : List[main.scala.apps.ScoobyFigure] = List(ScoobyFigure(1,0,1,0), 
                                                  //| ScoobyFigure(1,0,1,1), ScoobyFigure(1,0,1,0), ScoobyFigure(0,1,0,0), Scooby
                                                  //| Figure(0,1,1,0), ScoobyFigure(0,0,1,0))
  
  //Calculating Parent Entropy
  val (childEntropy_1, childEntrpy_2, avgEntropy) = ent(stickFigures, "label");
                                                  //>  7,  5
                                                  //| childEntropy_1  : Double = 0.9798687566511528
                                                  //| childEntrpy_2  : Double = 0.9798687566511528
                                                  //| avgEntropy  : Double = 0.9798687566511528
  //Calculating BodyType Entropy
  val (bodyEntropy_1, bodyEntrpy_2, avgBodyEntropy) = ent(stickFigures, "bodyType");
                                                  //> bodyEntropy_1  : Double = 0.9544340029249649
                                                  //| bodyEntrpy_2  : Double = 1.0
                                                  //| avgBodyEntropy  : Double = 0.9696226686166431
  
   //Calculating HeadType Entropy
  val (headEntropy_1, headEntrpy_2, avgHeadEntropy) = ent(stickFigures, "headType");
                                                  //> headEntropy_1  : Double = 0.6500224216483541
                                                  //| headEntrpy_2  : Double = 0.9182958340544896
                                                  //| avgHeadEntropy  : Double = 0.7841591278514218
  
  //Calculating ColorType Entropy
  val (colorEntropy_1, colorEntrpy_2, avgColorEntropy) = ent(stickFigures, "colorType");
                                                  //> colorEntropy_1  : Double = 0.9544340029249649
                                                  //| colorEntrpy_2  : Double = 1.0
                                                  //| avgColorEntropy  : Double = 0.9696226686166431
                                                  
   //Calculating Body-Rectangle-Color Entropy
  val (rectColorEntropy_1, rectColorEntrpy_2, avgRectColorEntropy) = ent(stickFigures, "body-rectangle-color");
                                                  //> rectColorEntropy_1  : Double = 0.0
                                                  //| rectColorEntrpy_2  : Double = 0.9709505944546686
                                                  //| avgRectColorEntropy  : Double = 0.6068441215341679
  
   //Calculating Body-Oval-Head Entropy
  val (ovalHeadEntropy_1, ovalHeadEntrpy_2, avgOvalHeadEntropy) = ent(stickFigures, "body-oval-head");
                                                  //> ovalHeadEntropy_1  : Double = 0.0
                                                  //| ovalHeadEntrpy_2  : Double = 0.9182958340544896
                                                  //| avgOvalHeadEntropy  : Double = 0.6887218755408672
  
  //Information Gain for BodyType
  val igbodyType = informationGain(avgEntropy, avgBodyEntropy)
                                                  //> igbodyType  : Double = 0.01024608803450966
  
  //Information Gain for HeadType
  val igheadType = informationGain(avgEntropy, avgHeadEntropy)
                                                  //> igheadType  : Double = 0.19570962879973097
  
  //Information Gain for ColorType
  val igcolorType = informationGain(avgEntropy, avgColorEntropy)
                                                  //> igcolorType  : Double = 0.01024608803450966
  
   //Information Gain for Body-Rectangle-Color
  val igrecColorType = informationGain(avgEntropy, avgRectColorEntropy)
                                                  //> igrecColorType  : Double = 0.3730246351169849
  
   //Information Gain for Body-Oval-Head
  val igovalHeadType = informationGain(avgEntropy, avgOvalHeadEntropy)
                                                  //> igovalHeadType  : Double = 0.29114688111028564
  
  print (igbodyType)                              //> 0.01024608803450966
  print (igheadType)                              //> 0.19570962879973097
  print (igcolorType)                             //> 0.01024608803450966
  print (igrecColorType)                          //> 0.3730246351169849
  print (igovalHeadType)                          //> 0.29114688111028564
 
  
  
  
}
case class ScoobyFigure(label: Int, bodyType: Int, headType: Int, colorType: Int){}