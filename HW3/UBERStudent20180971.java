import scala.Tuple2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.IOException;
import java.util.*;
import java.text.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class UBERStudent20180971 {
	public static void main(String[] args) throws Exception{
		if(args.length < 1){
			System.err.println("Usage: UBERStudent20180971 <file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession
			.builder()
			.appName("UBERStudent20180971")
			.getOrCreate();

		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		PairFunction<String, String, String> fmf = new PairFunction<String, String, String>() {
			public Tuple2<String,String> call(String s){
				String[] str = s.split(",");
				String value1 = str[0] + "," + returnDay(str[1]);
				String value2 =  str[3] + "," + str[2];
				return new Tuple2(value1, value2);
			}
		};
		JavaPairRDD<String, String> 	str1 = lines.mapToPair(fmf);
		

		Function2<String, String, String> f2 = new Function2<String, String, String>(){
			public String call(String x, String y){
				String[] word1 = x.split(",");
				String[] word2 = y.split(",");
				int num1 = Integer.parseInt(word1[0]) + Integer.parseInt(word2[0]);
				int num2 = Integer.parseInt(word1[1]) + Integer.parseInt(word2[1]); 
				String value = num1 + "," + num2;
				return value;
			}
		};

		JavaPairRDD<String, String> counts = str1.reduceByKey(f2);

		counts.saveAsTextFile(args[1]);
		spark.stop();
	}
	public static String returnDay(String date){ 
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy"); 
		String[] week = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"}; 
		Calendar cal = Calendar.getInstance(); 
		Date getDate; 
		try { 
			getDate = format.parse(date); 
			cal.setTime(getDate); 
			int w = cal.get(Calendar.DAY_OF_WEEK) - 1; 
			return week[w];
		} catch (ParseException e) { 
			e.printStackTrace(); 
		} catch (Exception e) { 
			e.printStackTrace(); 
		} 
		return "0";
	}
}
