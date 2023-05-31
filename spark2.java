package spark;



import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


public class spark2 {

	
	
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setAppName("SQL").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL")
				  .getOrCreate();

		
		List<StructField> fields = new ArrayList<>();
		StructField field1 = DataTypes.createStructField("id", DataTypes.StringType, false);
		StructField field2 = DataTypes.createStructField("timestamps", DataTypes.TimestampType, false);
		StructField field3 = DataTypes.createStructField("kilometers", DataTypes.DoubleType, false);
		StructField field4 = DataTypes.createStructField("regionid", DataTypes.IntegerType, false);

		fields.add(field1);
		fields.add(field2);
		fields.add(field3);
		fields.add(field4);
		
		StructType schema = DataTypes.createStructType(fields);
		
		Dataset<Row> Df = spark.read().format("csv").option("delimeter","\t").schema(schema).load("/home/dimitris/Desktop/Workspace/db2_project_data.csv");
		Dataset<Row> Df2 = Df.groupBy("regionid").avg("kilometers");
		Df2.show(54);
		
		
}
}



