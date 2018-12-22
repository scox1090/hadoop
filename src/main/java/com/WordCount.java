package com;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	
	public static class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		private final static LongWritable lw = new LongWritable();
		private Text word = new Text();
		
		@Override //이 오너테이션을 썻을 때 오류가 나면 제대로 안 된 것
		public void map(LongWritable ke, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();	//이렇게 하면 대소문자를 가리지 않는다고 함
			StringTokenizer st = new StringTokenizer(line, "\t\r\n\f|,.()<>"); 
			//line에서 저런 것도 다 잘라버림
			while(st.hasMoreTokens()) {
				word.set(st.nextToken().toLowerCase());
				context.write(word,lw);	//add throws 누러줌
			}
		}
	} //Mapper : hadoop에 기본 내장 되어 있음
	
	public static class MCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		private LongWritable lw = new LongWritable();
		
		protected void recude(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for(LongWritable value:values) {
				sum += value.get();
			}
			lw.set(sum);
			context.write(key, lw);
		}
	} //Map Reduce
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(MCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("word.txt"));
		FileOutputFormat.setOutputPath(job, new Path("word.log"));
		job.waitForCompletion(true);
		
	}
}
