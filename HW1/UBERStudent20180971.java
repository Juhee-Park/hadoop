import java.io.IOException;
import java.util.*;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.text.ParseException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20180971 
{

	public static class UBERMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text key_word = new Text();
		private Text value_word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			while (itr.hasMoreTokens()) 
			{
				//차량 번호
				String base_number = itr.nextToken();
				
				//날짜
				String inputDate = itr.nextToken();	
				
				//날짜 형식에 맞게 포멧에 집어넣음
				DateFormat df = new SimpleDateFormat("MM/dd/yyyy");
				
				Date date = new Date();
				try{
					date = df.parse(inputDate);
				} 
				catch (ParseException e) {
					System.err.println("error");
				}
				
				//Calendar클래스의 DAY_OF_WEEK를 통해 요일 숫자를 계산
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				
				String week_num = Integer.toString(cal.get(Calendar.DAY_OF_WEEK) -1);
				
				//일요일을 맨 뒤로
				//if (week_num.equals("-1")) {
				//	week_num = "6";
				//}
											
				String active_vehicles = itr.nextToken().trim();
				String trips = itr.nextToken().trim();				
				
				//key = <차량 번호,요일(숫자)>, value = <trips,active vehicles>
				//요일을 숫자로 넘김으로써 월요일부터 순서대로 정렬가능 			
				key_word.set(base_number + "," + week_num);
				value_word.set(trips + "," + active_vehicles);
				
				context.write(key_word, value_word);				
			}
		}
	}

	public static class UBERReducer extends Reducer<Text, Text, Text, Text> 
	{
		private String [] weeks = {"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"};
		private Text new_key = new Text();
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int sum_trips = 0;
			int sum_active_vehicles = 0;
			
			//key를 수정
			StringTokenizer itr_key = new StringTokenizer(key.toString(), ",");
			while (itr_key.hasMoreTokens())
			{
				String base_number = itr_key.nextToken();
				int weeks_num = Integer.parseInt(itr_key.nextToken().trim());
			
				
				//요일 번호를 요일 문자로 바꿔줌
				String week = weeks[weeks_num];
				new_key.set(base_number + "," + week);
			}
			
			for (Text val : values) 
			{
				//Text 형식으로 넘겨받은 value 값을 trips과 active vehicles로 나눈 뒤
				//총 합을 구해줌
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				while (itr.hasMoreTokens())
				{
					
					int trips = Integer.parseInt(itr.nextToken().trim());
					int active_vehicles = Integer.parseInt(itr.nextToken().trim());
					
					sum_trips += trips;
					sum_active_vehicles += active_vehicles;			
				}				
			}
			
			String sum = Integer.toString(sum_trips) + "," 
				+ Integer.toString(sum_active_vehicles);
						
			result.set(sum);
			context.write(new_key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: UBER <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "IMDb");	
		job.setJarByClass(UBERStudent20180971.class);
		
		job.setMapperClass(UBERMapper.class);
		job.setReducerClass(UBERReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
