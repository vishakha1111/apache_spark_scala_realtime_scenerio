import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object WordCount extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "wordcount")
  
  val input = sc.textFile("D:/VISHAKHA/BIG DATA COURSE/Week 9/search_data.txt")
  
  val words = input.flatMap(x => x.split(" "))
  
  val wordMap = words.map(x => (x, 1))
  
  val finalCount = wordMap.reduceByKey((x,y) => x + y )
  
  finalCount.collect.foreach(println)
    
  scala.io.StdIn.readLine()
}