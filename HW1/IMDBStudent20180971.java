import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20180971
{

	public static class IMDbMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text genre_list = new Text();
		private Text genre = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			//StringTokenizer가 작동하도록 문자 2개를 다른 하나의 문자로 바꿈
			String value_string = value.toString().replaceAll("::", "@");
			
			//바꾼 문자를 기준으로 토큰 분리
			StringTokenizer itr = new StringTokenizer(value_string, "@");
			while (itr.hasMoreTokens()) 
			{
			
				//영화 번호
				int index = Integer.parseInt(itr.nextToken().trim());
				//영화 제목
				String movie_name = itr.nextToken();				
				//영화 장르 목록
				genre_list.set(itr.nextToken());
	
				StringTokenizer genre_itr = new StringTokenizer(genre_list.toString(), "|");
				
				//목록을 나눈 뒤 1과 함께 매핑
				while (genre_itr.hasMoreTokens()) {
					genre.set(genre_itr.nextToken());
					context.write(genre, one);
				}				
			}
		}
	}

	public static class IMDbReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			// value의 총합 계산
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: IMDb <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "IMDb");	
		job.setJarByClass(IMDBStudent20180971.class);
		
		job.setMapperClass(IMDbMapper.class);
		job.setCombinerClass(IMDbReducer.class);
		job.setReducerClass(IMDbReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
