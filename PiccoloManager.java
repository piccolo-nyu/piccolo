package edu.nyu.cs.piccolo;

import org.apache.hadoop.hdfs.server.namenode.GetImageServlet;
import org.apache.hadoop.mapreduce.Job;

import edu.nyu.cs.piccolo.kernel.Kernel;

public class PiccoloManager {
	
	private static final PiccoloManager manager = new PiccoloManager();
	
	private PiccoloManager(){
	}
	
	public static PiccoloManager GetManager(){
		return manager;
	}
	
	// results of the computation can be written out here
	/*FileSystem hdfs = FileSystem.get(context.getConfiguration());
	for (int i = 0; i < kernels.length; i++) {
		kernels[i].writeOutKernelTable(hdfs, new Path(kernels[i].getName() + Math.random()*10000));
	}*/
}
