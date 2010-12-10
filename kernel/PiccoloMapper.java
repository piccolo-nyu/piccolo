package edu.nyu.cs.piccolo.kernel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.acl.Permission;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.hash.Hash;
import org.apache.tools.ant.types.CommandlineJava.SysProperties;

import edu.nyu.cs.piccolo.PiccoloConstants;
import edu.nyu.cs.piccolo.PiccoloManager;
import edu.nyu.cs.piccolo.PiccoloProtocol;
import edu.nyu.cs.piccolo.PiccoloWorker;
import edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair;
import edu.nyu.cs.piccolo.test.piccoloworker;

/**
 * The computation step for a Piccolo Job
 * Outputs of PiccoloMapper are written into tables. Which will later written to disk directly. 
 */

public abstract class PiccoloMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements PiccoloConstants{
	
	PiccoloProtocol client;
	
	/**
	 * Called once at the beginning of the task.
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		//kernels = setupKernels();
		client =  (PiccoloProtocol) RPC.waitForProxy(PiccoloProtocol.class, PiccoloProtocol.versionID, new InetSocketAddress("beaker-10", PiccoloWorkerServerPort), new Configuration());
	}
	
	
	/**
	 * Piccolo Mapper does not write out anything for Hadoop processing
	 * instead kernelFunction is used 
	 */
	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		return;
	}

	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// results of the computation can be written out here
		/*FileSystem hdfs = FileSystem.get(context.getConfiguration());
		for (int i = 0; i < kernels.length; i++) {
			kernels[i].writeOutKernelTable(hdfs, new Path(kernels[i].getName() + Math.random()*10000));
		}*/
		RPC.stopProxy(client);
	}

	/**
	 * Expert users can override this method for more complete control over the
	 * execution of the Mapper.
	 * 
	 * @param context
	 * @throws IOException
	 */
	public void run(Context context) throws IOException, InterruptedException {
		
		//if I am the # 1 then start other workers, else do hebelek hebelek .. 
		//table's partition function will be used here to send out data if it is on another host
		
		setup(context);
		while (context.nextKeyValue()) {
			client.putInTable(new Text("word-count"), new Text("yeni"), new IntWritable(4));
			/*
			for (int i = 0; i < PiccoloWorker.set.length; i++) {
				TablePair pair = PiccoloWorker.getKernels()[i].kernelfunction(context.getCurrentKey(), context.getCurrentValue());
				//if pair.key belong to partition in this worker
				// kernels[i].kernelTable().partitionFunction(pair.getValue(), pair.getValue(), context.NUM_NODES) == this node
				PiccoloWorker.getKernels()[i].getTable().put(pair.getKey(), pair.getValue());
				// else
					//send to corresponding worker
				//kernels[i].getTable().put(new Text("yasemin"), new IntWritable(1));
			}
			*/
		}
		cleanup(context);
	}
	

	//public abstract void setupKernels();

}