package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.*;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class BigramFrequent extends Configured implements Tool {
public static void main(String[] args) throws Exception {
int res = ToolRunner.run(new BigramFrequent(), args);
System.exit(res);
}

public int run(String[] args) throws Exception {

Job job = Job.getInstance(getConf(), "wordcount");
job.setJobName(BigramFrequent.class.getSimpleName());
job.setJarByClass(this.getClass());
job.setReducerClass(Reduce.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
job.setMapperClass(Map.class);
job.setCombinerClass(Reduce.class);
job.setReducerClass(Reduce.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
return job.waitForCompletion(true) ? 0 : 1;

}

public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
private final static IntWritable one = new IntWritable(1);
private Text word = new Text();
private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

@Override
public void map(LongWritable offset, Text lineText, Context context)
	throws IOException, InterruptedException {

	String line = ((Text) lineText).toString();
	StringTokenizer iter = new StringTokenizer(line); 
	String first = "";
	String second = "";
	if(iter.hasMoreTokens())
	{
		first = iter.nextToken();
	}
	while (iter.hasMoreTokens())
	{
		second=iter.nextToken();
		word.set("word 1 "+first+ " & "+"word 2" + second);
		context.write(word,one);
		first = second;
	}
	}
	}	

public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

private final static IntWritable SUM = new IntWritable();

@Override
public void reduce(Text word, Iterable<IntWritable> counts, Context context)
throws IOException, InterruptedException {


	Iterator<IntWritable> iter = counts.iterator();
	int sum=0;
	while (iter.hasNext())
	{
		sum+=iter.next().get();

	}
	SUM.set(sum);
	if (SUM.get()>3000)
	{
		context.write(word,SUM);
	}
	

}
}
}













































