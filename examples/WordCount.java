package edu.nyu.cs.piccolo.examples;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;

import nyu.cs.webgraph.LinkGraphRecordReader;
import nyu.cs.webgraph.RawGraphToLinkGraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.nyu.cs.piccolo.kernel.Kernel;
import edu.nyu.cs.piccolo.kernel.PiccoloMapper;
import edu.nyu.cs.piccolo.kernel.PiccoloTable;
import edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair;

public class WordCount {
	
	public static class WCKernel extends PiccoloMapper<Text, Text, Text, IntWritable> {

		@Override
		public Kernel[] setupKernels() {
			
			Kernel[] retk = new Kernel[1];
			
			Kernel<Text, Text, Text, IntWritable> MRKernel = new Kernel<Text, Text, Text, IntWritable>() {

				@Override
				public TablePair<Text, IntWritable> kernelfunction(Text key, Text value) {
					return new TablePair(key, new IntWritable(1));
				}

				@Override
				public PiccoloTable<Text, IntWritable> kernelTable() {
					PiccoloTable<Text, IntWritable> rett = new PiccoloTable<Text, IntWritable>() {
						
						@Override
						public IntWritable accumulator(IntWritable currentVal,IntWritable newVal) {
							if (currentVal != null)
								return new IntWritable(currentVal.get() + newVal.get());
							else
								return newVal;
						}
					};
					return rett;
				}
			};
			
			retk[0] = MRKernel;  
			return retk;
		
		}

	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "piccolo word count");
		job.setJarByClass(WordCount.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(WCKernel.class);
		//job.setPartitionerClass(SortedUrlHashPartitioner.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
