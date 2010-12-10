package edu.nyu.cs.piccolo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import edu.nyu.cs.piccolo.kernel.PiccoloTable;

public class PiccoloMaster implements PiccoloConstants {
	
	static PiccoloProtocol client = null; 
	
	public static void main(String[] args) throws IOException {
		client =  (PiccoloProtocol) RPC.waitForProxy(PiccoloProtocol.class, PiccoloProtocol.versionID, new InetSocketAddress("beaker-10", PiccoloWorkerServerPort), new Configuration());
		client.putInTable(new Text("word-count"), new Text("yeni"), new IntWritable(4));
		RPC.stopProxy(client);
	}
	
}
