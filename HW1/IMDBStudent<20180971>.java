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

public class IMDb 
{

	public static class IMDbMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text genre_list = new Text();
		private Text genre = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String value_string = value.toString().replaceAll("::", "@");
			StringTokenizer itr = new StringTokenizer(value_string, "@");
			while (itr.hasMoreTokens()) 
			{
				int index = Integer.parseInt(itr.nextToken().trim());
				System.out.println(index);
				String movie_name = itr.nextToken();
				System.out.println(movie_name);
				genre_list.set(itr.nextToken());
				System.out.println(genre_list);
				StringTokenizer genre_itr = new StringTokenizer(genre_list.toString(), "|");
				
				while (genre_itr.hasMoreTokens()) {
					String adasasd = genre_itr.nextToken();
					genre.set(adasasd);
					System.out.println(adasasd);
					context.write(genre, one);
				}				
			}
		}
	}

	public static class IMDbReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
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
		job.setJarByClass(IMDb.class);
		
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
