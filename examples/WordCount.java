package edu.nyu.cs.piccolo.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import nyu.cs.webgraph.LinkGraphRecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.nyu.cs.piccolo.PiccoloManager;
import edu.nyu.cs.piccolo.kernel.Kernel;
import edu.nyu.cs.piccolo.kernel.PiccoloMapper;
import edu.nyu.cs.piccolo.kernel.PiccoloTable;
import edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair;

public class WordCount {
	
	public static class TabSplitInputFormat extends FileInputFormat<Text, Text> {

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			RecordReader<Text, Text> rr = new LinkGraphRecordReader();
			rr.initialize(split, context);
			return rr;
		}

		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
			return codec == null;
		}
	}
	
	public static class WCKernel extends PiccoloMapper<Text, Text, Text, IntWritable> {
	}
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.child.java.opts", "-Xmx512m");
		conf.set("mapred.tasktracker.map.tasks.maximum", "1");
		conf.set("mapred.map.tasks", "1");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: piccolo-wordcount <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "piccolo word count");
		job.setInputFormatClass(TabSplitInputFormat.class);
		job.setNumReduceTasks(0);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WCKernel.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//job.setPartitionerClass(SortedUrlHashPartitioner.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//PiccoloManager.GetManager().initialize(job, setupKernels());
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
