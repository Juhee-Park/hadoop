import java.io.IOException;
import java.util.*;
import java.io.*;

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
	
	
	//평점 비교
	public static class RatingComparator implements Comparator<IMDB> {
		public int compare(IMDB x, IMDB y) {
			if ( x.rate > y.rate ) return 1;
			if ( x.rate < y.rate ) return -1;
			return 0;
		}
	}
	public static class IMDB{
		public String movie_name;
		public double rate;

		public IMDB(String movie_name, double rate){
			this.movie_name = movie_name;
			this.rate = rate;
		}
	}

	//비교 후 숫자만큼 큐에 넣기
	public static void insertIMDBtoQ(PriorityQueue q, IMDB i, int topK) {
		IMDB head = (IMDB) q.peek();
		if ( q.size() < topK || head.rate < i.rate )
		{
			q.add( i );
			if( q.size() > topK ) 
				q.remove();
		}
	}
	
	

	public static class IMDbMapper extends Mapper<Object, Text, Text, Text>
	{
		private boolean is_movie = false;
		private boolean is_rating = false;
		
		//private DoubleString double_key;
		private Text key_text = new Text();
		private Text value_text = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			boolean is_fantasy = false;
			//StringTokenizer가 작동하도록 문자 2개를 다른 하나의 문자로 바꿈
			String value_string = value.toString().replaceAll("::", "@");
			
			//바꾼 문자를 기준으로 토큰 분리
			StringTokenizer itr = new StringTokenizer(value_string, "@");
			while (itr.hasMoreTokens()) 
			{	
				if (is_movie)
				{
					//영화 번호
					String movie_id = itr.nextToken().trim();
					//영화 제목
					String movie_name = itr.nextToken();				
					//영화 장르 목록
					String genre_list = itr.nextToken();				
					StringTokenizer genre_itr = new StringTokenizer(genre_list.toString(), "|");
				
					//장르가 판타지일 때 reduce로 
					while (genre_itr.hasMoreTokens()) {				
						if("Fantasy".equals(genre_itr.nextToken())) 
						{
							is_fantasy = true;	
						}
					}
					
					if (is_fantasy) {
						key_text.set(movie_id);
						value_text.set(movie_name+"@");
					}		
				}
				
				if (is_rating)
				{
					//이용자 번호
					int user_id = Integer.parseInt(itr.nextToken().trim());
					//영화 번호
					String movie_id = itr.nextToken().trim();
					//평점
					String rate = itr.nextToken().trim();
					
					itr.nextToken();
					
					key_text.set(movie_id);
					value_text.set(rate);
				}
				
				context.write(key_text, value_text);
			}
		}
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			
			// 현재 처리중인 파일 종류 (A or B)
			if( filename.equals( "movies.dat" ) ) 
				is_movie = true;
			
			if( filename.equals( "ratings.dat" ) ) 
			is_rating = true;

		}
	}

	public static class IMDbReducer extends Reducer<Text, Text, Text, Text> 
	{	
		private PriorityQueue<IMDB> queue;
		private int topK;
		private Comparator<IMDB> comp = new RatingComparator();
		
		private Text id = new Text();
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
		
			boolean is_f = false;
			String movie_name = " ";
			
			int count = 0;
			int sum = 0;
			
			for (Text val : values) 
			{
				if(val.toString().indexOf("@") != -1) {
					movie_name = val.toString().replaceAll("@", "");
					is_f = true;
				}
				else if(!val.toString().equals("")) {
						sum += Integer.parseInt(val.toString());
						count++;
				}
			}
			

			if(is_f) {
				
				double avg = (double) sum / count;
				
				insertIMDBtoQ(queue, new IMDB(movie_name, avg), topK);
			
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<IMDB>( topK , comp);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while( queue.size() != 0 ) {
				IMDB i = (IMDB) queue.remove();
				
				String text = String.format("%.1f", i.rate);
				
				id.set(i.movie_name);
				result.set(text);
			
				context.write(id, result);
				
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: IMDb <in> <out>");
			System.exit(2);
		}
		
		int topK = Integer.parseInt(otherArgs[2]);
		conf.setInt("topK", topK);
		
		Job job = new Job(conf, "IMDb");	
		job.setJarByClass(IMDBStudent20180971.class);
		
		job.setMapperClass(IMDbMapper.class);
		job.setReducerClass(IMDbReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
