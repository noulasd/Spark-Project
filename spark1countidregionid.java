package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;



public class spark1countidregionid {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setAppName("CountIDRegionID").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = jsc.textFile("/home/dimitris/Desktop/Workspace/db2_project_data.csv",4);

		JavaPairRDD<String, Integer> regions = lines.mapToPair(s -> {String[] foo= s.split(","); return new Tuple2<>(foo[3], 1);
		});
		
		JavaPairRDD<String, Integer> result2 = regions.reduceByKey((x, y) -> (x+y));

		
		result2.glom().collect().forEach(x->System.out.println(x));

		jsc.close();
	}

}
