import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object FriendsByAge extends App{ 
  
  def parseLine(line: String) = {
   val fields = line.split("::") 
   val age = fields(2).toInt
   val numFriends = fields(3).toInt
   (age, numFriends) 
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "FriendsByAge")
  
  val input = sc.textFile("D:/VISHAKHA/BIG DATA COURSE/Week 9/friends-data.csv")
  
  val mappedInput = input.map(parseLine)
  
  val mappedFinal = mappedInput.map(x => (x._1, (x._2, 1)))
  
  //OR
  //val mappedFinal1 = mappedInput.mapValues(x => (x,1))
  
  val totalByAge = mappedFinal.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  
  val averagesByAge = totalByAge.map(x => (x._1, x._2._1/x._2._2)).sortBy(x => x._2)
  //OR
  //val averagesByAge1 = totalByAge.mapValues(x => (x._1/x._2))
  
  averagesByAge.collect.foreach(println)
  
     
}