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
  
  val reversedTuple = finalCount.map(x => (x._2, x._1))
  
  val sortedresults = reversedTuple.sortByKey(false).map(x => (x._2, x._1))
  
  val results = sortedresults.collect
  
  for(result <- results){
    val word = result._1
    val count = result._2
    println(s"$word : $count")
  }
    
  scala.io.StdIn.readLine()
}
