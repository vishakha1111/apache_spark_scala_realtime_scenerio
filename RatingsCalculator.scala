import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object RatingsCalculator extends App {
    
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]", "Totalspent")
  
  val input = sc.textFile("D:/VISHAKHA/BIG DATA COURSE/Week 9/movie-data.data")
  
  val mappedInput = input.map(x => x.split("\t")(2))
  
  val results = mappedInput.countByValue
  results.foreach(println)
  
  //val ratings = mappedInput.map(x => (x, 1))
  
  //val reducedRatings = ratings.reduceByKey((x,y) => x + y)
  
  
  
  //val results = reducedRatings.collect
  
  results.foreach(println)
  
}