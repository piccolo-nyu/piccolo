package edu.nyu.cs.piccolo;

import java.awt.RenderingHints.Key;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

import edu.nyu.cs.piccolo.kernel.Kernel;
import edu.nyu.cs.piccolo.kernel.PiccoloTable;
import edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair;

//Piccolo Server
public class PiccoloWorker implements PiccoloProtocol, PiccoloConstants, Runnable {

	private static final Log LOG = LogFactory.getLog(PiccoloWorker.class.getName());
	private final Hashtable<String, Kernel> kernels = new Hashtable<String, Kernel>();
	private boolean isRunning = false;
	InetAddress ip;
	
	
	//private static PiccoloWorker piccoloWorker = null;

	public static Server ipcServer = null;
	//PiccoloProtocol client = null; 
	
	public PiccoloWorker() throws IOException {

		Kernel[] kernels = setupKernels();
		for (int i = 0; i < kernels.length; i++) {
			addKernel(kernels[i].getName(), kernels[i]);
		}
		try {
			ip = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			LOG.error("fetching local ip: " + e.toString());
			return;
		}
		
		startPiccoloWorker();
	}

/*	public static PiccoloWorker getPiccoloWorker() {
		return piccoloWorker;
	}

	public static void setPiccoloWorker(PiccoloWorker w) {
		piccoloWorker = w;
	}
*/
	private Kernel getKernel(String kernelName) {
		return kernels.get(kernelName);
	}

	private void addKernel(String name, Kernel k) {
		kernels.put(name, k);
	}

	protected void put(String kernelName, Object key, Object value) {
		if (getKernel(kernelName) == null) {
			LOG.error("kernel " + kernelName + "does not exists! ");
			return;
		}
		kernels.get(kernelName).addToTable(key, value);
	}

	public void startPiccoloWorker() throws IOException{
		startIPCServer();
	}
	
	public static void stopPiccoloWorker() throws IOException{
		stopIPCServer();
	}

	public boolean isRunning() {
		return isRunning;
	}

	private synchronized void startIPCServer() {
		// init ipc server
		Configuration conf = new Configuration();
		try {
			ipcServer = RPC.getServer(PiccoloWorker.class, this, ip.getHostName(), PiccoloWorkerServerPort, PiccoloWorkerServerHandlerCount, false, conf, null);
		} catch (IOException e) {
			LOG.error("can noy initiate ipc serevr " + e.toString());
		}
		try {
			ipcServer.start();
		} catch (IOException e) {
			LOG.error("can noy start ipc serevr " + e.toString());
		}
	}
	
	private static void stopIPCServer() throws IOException {
		if (ipcServer != null) {
			ipcServer.stop();
			ipcServer = null;
		}
	}

	public static void main(String[] args) {
		
		//start all piccolo workers
		/*try {
			new PiccoloWorker().run();
		} catch (IOException e) {
			LOG.error("starting ipc server: " + e.toString());
		}*/
		
		//stop all piccolo workers
		try {
			stopIPCServer();
		} catch (IOException e) {
			LOG.error("starting ipc server: " + e.toString());
		}
	}
	
	
	/*public static void main(String[] args) {

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
			pw.println("hostname: " + thisIp.getHostName());
			if (pw != null)
				pw.close();
			if (fos != null)
				fos.close();
		} catch (IOException e) {
			LOG.error("creating file writer: " + e.toString());
		}
	}*/
	
	
	private Kernel[] setupKernels() {

		Kernel[] kernels = new Kernel[1];

		Kernel<Text, Text, Text, IntWritable> WCKernel = new Kernel<Text, Text, Text, IntWritable>("word-count") {

			@Override
			public TablePair<Text, IntWritable> kernelfunction(Text key, Text value) {
				return new TablePair<Text, IntWritable>(key, new IntWritable(1));
			}

			@Override
			public PiccoloTable<Text, IntWritable> initializeKernelTable() {
				PiccoloTable<Text, IntWritable> rett = new PiccoloTable<Text, IntWritable>() {
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
				return rett;
			}

			@Override
			public void writeOutKernelTable(FileSystem fs, Path path) throws IOException {
				FSDataOutputStream dos = fs.create(path);

				Set<Map.Entry<Text, IntWritable>> elements = this.getTableEntrySet();
				Iterator<Map.Entry<Text, IntWritable>> itr = elements.iterator();
				while (itr.hasNext()) {
					Map.Entry<Text, IntWritable> s = itr.next();
					dos.writeBytes(s.getKey().toString() + "\t" + s.getValue().get() + "\n");
				}
				dos.close();
			}
		};
		kernels[0] = WCKernel;
		return kernels;
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		if (protocol.equals(PiccoloProtocol.class.getName())) 
			return PiccoloProtocol.versionID;
		throw new IOException("Unknown protocol to " + getClass().getSimpleName() + ": " + protocol);
	}

	@Override
	public void putInTable(Text tableName, Text key, IntWritable value) throws IOException {
		put(tableName.toString(), key, value);
	}

	public static PiccoloProtocol createPiccoloProtocolProxy(String hostname) throws IOException {
		InetSocketAddress addr = NetUtils.createSocketAddr(hostname + ":" + PiccoloWorkerServerPort);
		if (PiccoloProtocol.LOG.isDebugEnabled()) {
			PiccoloProtocol.LOG.info("PiccoloProtocol addr=" + addr);
		}
		return (PiccoloProtocol) RPC.getProxy(PiccoloProtocol.class, PiccoloProtocol.versionID, addr, null);
	}

	@Override
	public void run() {
		/*while (true)
		{
			
		}*/
		
	}

	@Override
	public void writeToLocalFileSystem(String filename, String dir) {
		String basedir = dir;
		// dir = "/home/yavcular/"
		String hosaddress = ip.getHostName();
		File file = new File(basedir + hosaddress + ".txt");
		try {
			boolean b = file.createNewFile();
			if (!b) {
				System.out.println(basedir + hosaddress + ".txt already exist!");
				return;
			}
		} catch (IOException e) {
			LOG.error("creating new file : " + e.toString());
		}

		FileWriter fos;
		try {
			fos = new FileWriter(file);
			PrintWriter pw = new PrintWriter(fos, true);
			pw.println("num of tables: " + kernels.size());
			String[] names = (String[]) kernels.keySet().toArray();
			for (int i = 0; i < names.length; i++) {
				pw.println(names[i]);
			}
			if (pw != null)
				pw.close();
			if (fos != null)
				fos.close();
		} catch (IOException e) {
			LOG.error("creating file writer: " + e.toString());
		}
		
	}
	
}
