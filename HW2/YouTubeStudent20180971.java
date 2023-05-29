//유튜브 랭킹 코드
import java.io.IOException;
import java.util.*;
import java.text.*;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20180971{
	
	public static class Youtube{
		public String Genre;
		public double rate;

		public Youtube(String Genre, double rate){
			this.Genre = Genre;
			this.rate = rate;
		}

		public String getString(){
			String s = String.format("%.4f", rate);
			return Genre + " " + s;
		}
	}




	public static class RatingComparator implements Comparator<Youtube> {
		public int compare(Youtube x, Youtube y) {
			if ( x.rate > y.rate ) return 1;
			if ( x.rate < y.rate ) return -1;
			return 0;
		}
	}

	public static void insertYoutube(PriorityQueue q, String Genre, double rate, int topK) {
		Youtube youtube_head = (Youtube) q.peek();
		if ( q.size() < topK || youtube_head.rate < rate )
		{
			Youtube youtube = new Youtube(Genre, rate);
			q.add( youtube );
			if( q.size() > topK ) 
				q.remove();
		}
	}


	public static class YoutubeMap extends Mapper<LongWritable , Text, Text, Text>
	{
		private Text word2 = new Text(); 
		private Text word = new Text();
		String str=null;
		String str1;
		String str2; 

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line,"|"); 
			String[] youtube = new String[10];	

			int i = 0;
			
			while(tokenizer.hasMoreTokens()){
				youtube[i] = tokenizer.nextToken().trim();
				i++;
			}
			
			str1 = youtube[3];
			str2 = youtube[i-1];
			word.set(str1);
			word2.set(str2);
			context.write(word, word2);
			
		}
	}


	public static class YoutubeReduce extends Reducer<Text, Text, Text, NullWritable>
	{	
		private Comparator<Youtube> comp = new RatingComparator();
		private PriorityQueue<Youtube> queue;
		private int topK;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double sum = 0;
			double i = 0;
			for(Text val : values){
				sum += Double.parseDouble(val.toString());
				i++;
			}
			String genre = key.toString();
			sum = sum / i;
			insertYoutube(queue, genre, sum, topK);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube>( topK , comp);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while( queue.size() != 0 ) {
			Youtube youtube = (Youtube) queue.remove();
			context.write( new Text( youtube.Genre), new Text( youtube.rate));
			}
		}
	}


	

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: TopK <in> <out> How many topK"); System.exit(2);
		}
		int topK = Integer.parseInt(otherArgs[2]);
		conf.setInt("topK", topK);
		Job job = new Job(conf, "YouTubeStudent20180971");

		job.setJarByClass(YouTubeStudent20180971.class);
		job.setMapperClass(YoutubeMap.class);
		job.setReducerClass(YoutubeReduce.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}
