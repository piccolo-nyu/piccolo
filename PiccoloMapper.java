package edu.nyu.cs.piccolo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.Mapper;

import edu.nyu.cs.piccolo.kernel.KernelFunctionDefinition;
import edu.nyu.cs.piccolo.kernel.PiccoloTable;
import edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair;

/**
 * The computation step for a Piccolo Job
 * Outputs of PiccoloMapper are written into tables. Which will later written to disk directly. 
 */

public abstract class PiccoloMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements PiccoloConstants {
	
	//private static final Log LOG = LogFactory.getLog(PiccoloMapper.class.getName());
	MapperClient clients;
	KernelFunctionDefinition[] kernelfunctions; 
	Buffer[] sendOutBuffer; 
	
	/**
	 * opens a connection to PiccoloWorker running on each node 
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		clients = MapperClient.getInstance(new Configuration());
		kernelfunctions = setupKernelFunctions();
		sendOutBuffer = new Buffer[clients.getNumOfHosts()];
		
		clients.startClients();
	}
	
	/**
	 * Does nothing ! 
	 */
	@SuppressWarnings("unchecked")
	protected void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		return;
	}
	
	/**
	 * closes all PiccoloClient connections
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		clients.stopClients();
	}

	/**
	 * PICCOLO MAPPER
	 * applies kernel all kernel functions on every <key, value> pair to mapper
	 * sends out the results corresponding node
	 * 
	 * @param context
	 * @throws IOException
	 */
	public void run(Context context) throws IOException, InterruptedException {
		
		setup(context);
		while (context.nextKeyValue()) {
			TablePair pair;
			int hosttosend;
			for (int j = 0; j < kernelfunctions.length; j++) {
				pair = kernelfunctions[j].kernelfunction(context.getCurrentKey(), context.getCurrentValue());
				hosttosend = kernelfunctions[j].hash(pair.getKey());
				//should check if it belongs to itself ! then send ! ?
				
				/*if (sendOutBuffer[hosttosend] == null)
					sendOutBuffer[hosttosend] = new Buffer(hosttosend);
				sendOutBuffer[hosttosend].addToBuffer(pair.getKey(), pair.getValue());
				*/
				// need to flush at some time ! 
				clients.getClientById(hosttosend).putInTable(new Text(kernelfunctions[j].getName()), pair.getKey(), pair.getValue());
			}
		}
		cleanup(context);
	}
	
	public abstract KernelFunctionDefinition[] setupKernelFunctions();
}


class Buffer{
	
	private int hostToSend; 
	private ArrayList<String> tablename; 
	private ArrayList<Writable> key;
	private ArrayList<Writable> value;
	
	public Buffer(int hostid){
		hostid = hostid;
		tablename = new ArrayList<String>(50);
		key = new ArrayList<Writable>(50);
		value = new ArrayList<Writable>(50);
	}
	
	public void addToBuffer(Writable k, Writable v)
	{
		key.add(k);
		value.add(v);
	}
	
	public Writable[] getKeys(){
		return (Writable[])key.toArray();
	}
	public Writable[] getValues(){
		return (Writable[])value.toArray();
	}
}