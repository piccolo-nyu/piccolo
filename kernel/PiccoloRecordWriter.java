package edu.nyu.cs.piccolo.kernel;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class PiccoloRecordWriter<KEY, VALUE> extends RecordWriter<KEY, VALUE> {

	@Override
	public void write(KEY key, VALUE value) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}
}