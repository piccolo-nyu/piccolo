package edu.nyu.cs.piccolo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import edu.nyu.cs.piccolo.kernel.KernelTableDefinition;
import edu.nyu.cs.piccolo.kernel.PiccoloTable;

//Piccolo Server
public class PiccoloWorker implements PiccoloProtocol, PiccoloConstants {

	private static final Log LOG = LogFactory.getLog(PiccoloWorker.class.getName());
	private Hashtable<String, KernelTableDefinition> tables;
	private static InetAddress ip;

	private static Server ipcServer = null;
	private static PiccoloWorker piccolow; 

	private PiccoloWorker() throws IOException {
		try {
			ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			LOG.error("fetching local ip: " + e.toString());
			return;
		}
	}

	//should be defined seperately forevery piccolo job 
	private static KernelTableDefinition[] setupTables(){
		KernelTableDefinition<Text, IntWritable> wcTable = new KernelTableDefinition<Text, IntWritable>("word-count") {

			@Override
			public PiccoloTable<Text, IntWritable> kernelTable() {
				
				return new PiccoloTable<Text, IntWritable>(){
					
					@Override
					public IntWritable accumulator(IntWritable currentVal, IntWritable newVal) {
						if (currentVal != null)
							return new IntWritable(currentVal.get() + newVal.get());
						else
							return newVal;
					}
					
					@Override
					public String tablePairToString(TablePair<Text, IntWritable> pair) {
						return pair.getKey().toString() + "\t" + pair.getValue().get();
					} 
					
				};
			}
			
			/*
			@Override
			public void writeOutKernelTable(FileSystem fs, Path path) throws IOException {
				System.out.println("Nothing is written out !");
				
				FSDataOutputStream dos = fs.create(path);

				Set<Map.Entry<Text, IntWritable>> elements = this.getTableEntrySet();
				Iterator<Map.Entry<Text, IntWritable>> itr = elements.iterator();
				while (itr.hasNext()) {
					Map.Entry<Text, IntWritable> s = itr.next();
					dos.writeBytes(s.getKey().toString() + "\t" + s.getValue().get() + "\n");
				}
				dos.close();
			}*/
			
		};
		
		KernelTableDefinition[] tbls = {wcTable};
		return tbls;
	}
	

	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		if (protocol.equals(PiccoloProtocol.class.getName()))
			return PiccoloProtocol.versionID;
		throw new IOException("Unknown protocol to " + getClass().getSimpleName() + ": " + protocol);
	}

	@Override
	public void initialize(){
		tables = new Hashtable<String, KernelTableDefinition>();
		KernelTableDefinition[] tbls = setupTables();
		for (int i = 0; i < tbls.length; i++) {
			tables.put(tbls[i].getName(), tbls[i]);
		}
	}
	
	@Override
	public void putInTable(Text tableName, Writable key, Writable value) throws IOException {
		put(tableName.toString(), key, value);
	}
	
	private void put(String name, Writable key, Writable value) {
		if (tables.get(name) == null) {
			LOG.error("table " + name + " does not exists! ");
			return;
		}
		tables.get(name).addToTable(key, value);
	}

	@Override
	public void putInTable(Text tableName, Writable[] key, Writable[] value) throws IOException {
		if (key.length != value.length)
			LOG.error("key and value array length does not match!");
		else
		{
			for (int i = 0; i < value.length; i++) {
				putInTable(tableName, key[i], value[i]);
			}
		}
		
	}
	
	@Override
	public void writeToLocalFileSystem(String basedir) 
	{
		if (!basedir.endsWith("/"))
			basedir = basedir + "/";

		String hosaddress = ip.getHostName();
		File file = new File(basedir + hosaddress + "-0000.txt");
		try {
			boolean b = file.createNewFile();
			
			if (!b) {
				LOG.error((new Date()).toString() + ": " + file.getAbsolutePath().toString() + ".txt already exist!\n");
				return;
			}
		} catch (IOException e) {
			LOG.error("creating new file : " + e.toString());
		}
		
		System.out.println("openning file writers");
		FileWriter fos = null;
		PrintWriter pw = null;
		try {
			fos = new FileWriter(file);
			pw = new PrintWriter(fos, true);
			pw.println("num of tables: " + tables.size());
			pw.println("name of tables: " + tables.keySet().toString()); 
			System.out.println("openning file writers5");
			Enumeration<KernelTableDefinition> e = tables.elements();
			Iterator tmpi;
			KernelTableDefinition tmp;
			while (e.hasMoreElements()) {
				tmp = e.nextElement();
				pw.println("   ");
				pw.println("------" + tmp.getName().toUpperCase() + " (has" + tmp.getTableEntrySet().size() + " elements):\t ");
				tmpi = tmp.getTableEntrySet().iterator();
				while (tmpi.hasNext())
					pw.println(tmpi.next().toString());
			}
		} catch (IOException e) {
			LOG.error("creating file writer: " + e.toString());
		} finally {
			if (pw != null)
				pw.close();
			if (fos != null)
				try {
					fos.close();
				} catch (IOException e) {
					LOG.error("closing the file output writer");
				}
			System.out.println("hebelek hebelek!");

		}
	}
	
	public static PiccoloWorker getPiccoloWorker() throws IOException{
		if ( piccolow != null)
			return piccolow;
		else
		{
			piccolow = new PiccoloWorker();
			LOG.debug("static piccoloserver instance created!");
			return piccolow; 
		}
	}
	
	private static synchronized void startIPCServer() {
		// init ipc server
		Configuration conf = new Configuration();
		try {
			ipcServer = RPC.getServer(PiccoloWorker.class, getPiccoloWorker(), ip.getHostName(), PiccoloWorkerServerPort, PiccoloWorkerServerHandlerCount, false, conf, null);
		} catch (IOException e) {
			LOG.error("can not initiate ipc serevr " + e.toString());
		}
		try {
			ipcServer.start();
		} catch (IOException e) {
			LOG.error("can not start ipc serevr " + e.toString());
		}
	}

	private static void stopIPCServer(){
		if (ipcServer != null) {
			ipcServer.stop();
			ipcServer = null;
		}
	}
	
	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("usage: PiccoloWorker [ start|stop ]");
			System.exit(1);
		}

		if (args[0].equals("start")) // start all piccolo workers
		{
			startIPCServer();
		} else if (args[0].equals("stop")) // stop all piccolo workers
		{
			stopIPCServer();
		}
		else {
			System.err.println("Missing argument for PiccoloWorker\nusage: PiccoloWorker [ start|stop ]");
		}
	}

	public void dummyCall(Text t) {
		
		String basedir = "/home/yavcular/";
		InetAddress thisIp;
		try {
			thisIp = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			LOG.error("fetching local ip: " + e.toString());
			return;
		}
		String ip = thisIp.getHostAddress();
		File file = new File(basedir + ip.replace(".", "-") + ".txt");
		try {
			boolean b = file.createNewFile();
			if (!b) {
				System.out.println(basedir + ip.replace(".", "-") + ".txt already exist!");
				return;
			}
		} catch (IOException e) {
			LOG.error("creating new file : " + e.toString());
		}

		FileWriter fos;
		try {
			fos = new FileWriter(file);
			PrintWriter pw = new PrintWriter(fos, true);
			pw.println(t.toString());
			pw.println("hostname: " + thisIp.getHostName());
			if (pw != null)
				pw.close();
			if (fos != null)
				fos.close();
		} catch (IOException e) {
			LOG.error("creating file writer: " + e.toString());
		}
	}

}
