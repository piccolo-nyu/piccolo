package edu.nyu.cs.piccolo;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * An inter-datanode protocol for updating generation stamp
 */
@InterfaceAudience.Private
public interface PiccoloProtocol extends VersionedProtocol {
	public static final Log LOG = LogFactory.getLog(PiccoloProtocol.class);

	/**
	 * 1: initial version.
	 */
	public static final long versionID = 1L;

	/**
	 * Add one entry to one table
	 * @return 
	 */
	public void putInTable(Text tableName, Text key, IntWritable value) throws IOException;
	
	void writeToLocalFileSystem(String filename, String dir);
}