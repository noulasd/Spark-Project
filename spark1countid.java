package spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class spark1countid {

	public static int n = 0;
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("CountID").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = jsc.textFile("/home/dimitris/Desktop/Workspace/db2_project_data.csv",4);
		
		JavaPairRDD<String, Integer> ids = lines.mapToPair(s -> {String[] foo= s.split(","); return new Tuple2<>(foo[0], 1);
		});
		
		JavaPairRDD<String, Integer> result1 = ids.reduceByKey((x, y) -> (x+y));
		result1.foreach(x-> { n = n + Integer.parseInt(x._2().toString()); });
		System.out.println("The number of total ID's is: " + n + "\n");
		jsc.close();
		
	}
	
	
	
	
	
	
}
