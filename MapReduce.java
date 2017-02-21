import java.io.*;
import java.util.StringTokenizer;
import java.util.*;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;

public class MapReduce {
//javac -cp /usr/lib/hadoop/client-0.20/\*:/usr/lib/hadoop/\* Desktop/WordCount.java

	public static class sortingKey implements WritableComparable<sortingKey> {
		private String key;
		private String value;
		public sortingKey(String key, String value){
			this.key = key;
			this.value = value;
		}
		public sortingKey(){

		}
		public String getKey(){
			return key;
		}
		public String getValue(){
			return value;
		}
		public void setKey(String key){
			this.key = key;
		}
		public void setValue(String value){
			this.value = value;
		}
		public void write(DataOutput out) throws IOException {
         WritableUtils.writeString(out, key);
         WritableUtils.writeString(out, value);
       }
       
       public void readFields(DataInput in) throws IOException {
         key = WritableUtils.readString(in);
         value = WritableUtils.readString(in);
       }
       
       public int compareTo(sortingKey sortItem) {
       	Integer keynew = new Integer(key);
       	Integer valuenew = new Integer(value);
		 if (sortItem == null)
			return 0;
			int intcnt = new Integer(sortItem.value).compareTo(valuenew);
			return intcnt == 0 ? keynew.compareTo(new Integer(sortItem.key)) : intcnt;
		}
		@Override
		public String toString() {
			return key.toString() + ":" + value.toString();	
		}  

	}
	public static class sortingMapper extends Mapper<Object,Text, sortingKey,IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			try{
				String nextline;
				String[] eachword;
				eachword = value.toString().split("\\t");
				context.write(new sortingKey(eachword[0], eachword[1]), one);

			}
			catch(Exception e){

			}

		}
		
	}
	public static class sortingReducer extends Reducer<sortingKey, IntWritable, Text, Text>	{
		private IntWritable result = new IntWritable();
		public void reduce(sortingKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				Configuration conf = context.getConfiguration();
				Long topk = new Long(conf.get("TopK"));
				if(context.getCounter(CountingTopK.TOPK).getValue() < topk){

					context.write(new Text(key.getKey()), new Text(key.getValue()));					
					context.getCounter(CountingTopK.TOPK).increment(1);
				}
			
		}
	}
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try{

				Configuration conf = context.getConfiguration();
				Date timeStamp1= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(conf.get("TimeStamp1"));
				Date timeStamp2= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(conf.get("TimeStamp2"));
				String nextline;
				String[] eachword;
				Date condition;
				eachword = value.toString().split("\\s");
				
					if(eachword[0].equals("REVISION")){
						condition = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(eachword[4]);
						if(timeStamp1.before(condition) && timeStamp2.after(condition)){
							Text key1 = new Text(eachword[1]);
							context.write(key1,one);
					
						}
					}
				
			}
			catch(Exception e){
			}
		}
	}
	
	public static class IntSumReducer
		extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();				
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	public enum CountingTopK{
		TOPK
	}
	public static void main(String[] args) throws Exception {
	
			Configuration conf = new Configuration();
			conf.addResource(new Path("/local/bd4/bd4-hadoop-sit/conf/core-site.xml"));
			conf.set("mapred.jar", "file:///Desktop/MapReduce.jar");
			conf.set("TimeStamp1", args[2]);
			conf.set("TimeStamp2", args[3]);
			conf.set("TopK", args[4]);
			Job job = Job.getInstance(conf);
			job.setNumReduceTasks(1);
			job.setJarByClass(MapReduce.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path("OutputTest"));

			job.submit();
			if(job.waitForCompletion(true)){
				job = Job.getInstance(conf);
				job.setNumReduceTasks(1);
				job.setJarByClass(MapReduce.class);
				job.setMapperClass(sortingMapper.class);
				job.setReducerClass(sortingReducer.class);
				job.setMapOutputKeyClass(sortingKey.class);
				job.setMapOutputValueClass(IntWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job, new Path("OutputTest/part-r-00000"));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
			}
			System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}
