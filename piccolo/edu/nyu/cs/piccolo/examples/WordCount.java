package edu.nyu.cs.piccolo.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.nyu.cs.piccolo.MapperClient;
import edu.nyu.cs.piccolo.PiccoloMapper;
import edu.nyu.cs.piccolo.kernel.KernelFunctionDefinition;
import edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair;

public class WordCount {

	public static class WCKernel extends PiccoloMapper<Text, Text, Text, IntWritable> {

		@Override
		public KernelFunctionDefinition[] setupKernelFunctions() {
			KernelFunctionDefinition<LongWritable, Text, Text, IntWritable> wordcountfunc = new KernelFunctionDefinition<LongWritable, Text, Text, IntWritable>("word-count") {

				@Override
				public TablePair<Text, IntWritable> kernelfunction(LongWritable key, Text value) {
					String str = value.toString();
					if ( str.length() > 0)
						return new TablePair<Text, IntWritable>(new Text(str.substring(0, 1)), new IntWritable(1));
					return new TablePair<Text, IntWritable>(new Text(""), new IntWritable(1));
				}
				
				@Override
				public int hash(Text key) {
					int i = key.hashCode(); 
					if (Integer.signum(i) == -1)
						i = i*-1;
					return i % MapperClient.getInstance(new Configuration()).getNumOfHosts();
				}

			};

			KernelFunctionDefinition[] funcs = new KernelFunctionDefinition[1];
			funcs[0] = wordcountfunc;

			return funcs;
		}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		//System.out.println(MapperClient.getInstance(conf).getClientById(0));
		conf.set("mapred.child.java.opts", "-Xmx512m");
		conf.set("mapred.tasktracker.map.tasks.maximum", "1");
		conf.set("mapred.map.tasks", "1");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: piccolo-wordcount <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "piccolo word count");
		job.setNumReduceTasks(0);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WCKernel.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		MapperClient c = MapperClient.getInstance(conf);
		c.initialize();
		System.out.println("everyone should've initialized at this point!");
		boolean isSuccesful = job.waitForCompletion(true);
		
		c.flushTables("/home/yavcular/");
		
		System.exit(isSuccesful ? 0 : 1);
	}

}
