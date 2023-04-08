import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source

object KeywordAmount extends App{
  
  def loadBoringWords():Set[String] = {
    
    var boringWords:Set[String] = Set()
    
    val lines = Source.fromFile("D:/VISHAKHA/BIG DATA COURSE/Week 10/boringwords.txt").getLines()
    
    for(line <- lines){
      boringWords += line            //Putting each line to the set
    }
    boringWords                      //Returning set
  }
   
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "KeywordAmount")
  
  var nameSet = sc.broadcast(loadBoringWords)
  
  val initial_rdd = sc.textFile("D:/VISHAKHA/BIG DATA COURSE/Week 10/bigdatacampaigndata.csv")
  
  val mappedInput = initial_rdd.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  
  val words = mappedInput.flatMapValues(x => x.split(" "))
  
  val finalMapped = words.map(x => (x._2.toLowerCase(), x._1))
  
  //(big , 24)
  //(is , 50)
  val filteredRDD = finalMapped.filter(x => !nameSet.value(x._1))
  
  val total = filteredRDD.reduceByKey((x, y) => x+y)
  
  val sorted = total.sortBy(x => x._2, false)
  
  sorted.take(20).foreach(println) 

}