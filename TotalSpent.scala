import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object TotalSpent extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "Totalspent")
  
  val input = sc.textFile("D:/VISHAKHA/BIG DATA COURSE/Week 9/customer-orders.csv")
  
  val mappedInput = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat))
  
  val totalByCustomer = mappedInput.reduceByKey((x, y) => x+y)
  
  val result = totalByCustomer.sortBy(x => x._2)
  
  val finalOutput = result.collect
  
  finalOutput.foreach(println)
   
  scala.io.StdIn.readLine()
}