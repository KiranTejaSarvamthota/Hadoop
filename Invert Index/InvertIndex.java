package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.*;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertIndex extends Configured implements Tool {

public static void main(String[] args) throws Exception {
int res = ToolRunner.run(new InvertIndex(), args);
System.exit(res);
}

public static class Map extends Mapper<LongWritable, Text, Text, Text> {

@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException
 {
 	String file = ((FileSplit) context.getInputSplit()).getPath().getName();
	String line = value.toString();
	String word[] = line.split(" ");
	for (String w : word) 
	{
		context.write(new Text(w),new Text(file));
	}
 	}	
}

public static class Reduce extends Reducer<Text, Text, Text, Text> {
@Override
public void reduce(Text key, Iterable<Text> values, Context context)
throws IOException, InterruptedException 
{
int count = 0;
HashMap hashm = new HashMap();
for (Text val : values) 
	{
	String str = val.toString();
	if(hashm!=null && hashm.get(str)!=null)
	{
		count =(int)hashm.get(str);
		hashm.put(str,++count);
	}
	else
	{
		hashm.put(str,1); 
	}
	}
     	 context.write(key, new Text(hashm.toString()));
}
}

public int run(String[] args) throws Exception {

Job job = Job.getInstance(getConf(), "InvertIndex");
job.setJarByClass(this.getClass());
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);

return job.waitForCompletion(true) ? 0 : 1;

}

}
















