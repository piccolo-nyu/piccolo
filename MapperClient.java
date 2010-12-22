package edu.nyu.cs.piccolo;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;

public class MapperClient implements PiccoloConstants {
	
	private static MapperClient instance = null; 

	private PiccoloProtocol[] clients;
	private static String[] hostKeyMapping; 
	private static final Log LOG = LogFactory.getLog(MapperClient.class.getName());
	private static Configuration conf;

	private MapperClient(Configuration conf){
		this.conf = conf;
		buildHostKeyMapping();
		clients = new PiccoloProtocol[hostKeyMapping.length];
	}
	
	public static MapperClient getInstance(Configuration c){
		if ( instance == null )
			return new MapperClient(c);
		else
			return instance;
	}
	
	public void startClients(){
		for (int i = 0; i < hostKeyMapping.length; i++) {
			try {
				clients[i] =  (PiccoloProtocol) RPC.waitForProxy(PiccoloProtocol.class, PiccoloProtocol.versionID, new InetSocketAddress(hostKeyMapping[i], PiccoloWorkerServerPort), new Configuration());
			} catch (IOException e) {
				System.out.println("could not start the client!\n");
				e.printStackTrace();
			}
		}
	}
	
	public void stopClients(){
		for (int i = 0; i < clients.length; i++) {
			RPC.stopProxy(clients[i]);
		}
	}

	private void buildHostKeyMapping(){
		
		Cluster c = null;
		try {
			c= new Cluster(conf);
		} catch (IOException e) {
			LOG.error("can not create cluster with given configuration!");
		}
		
		TaskTrackerInfo[] info = null; 
		try {
			info = c.getActiveTaskTrackers();
		} catch (Exception e) {
			LOG.error("can not reach to active task trackers!");
		} 
		
		hostKeyMapping = new String[info.length];
		StringBuffer bf;
		for (int i = 0; i < info.length; i++) {
			bf = new StringBuffer(info[i].getTaskTrackerName());
			//sample ---> tracker_beaker-18.news.cs.nyu.edu:localhost/127.0.0.1:41390
			hostKeyMapping[i] = bf.substring(bf.indexOf("_")+1, bf.indexOf(".")).toString();
		}
	}
	
	public PiccoloProtocol getClientById(int id){
		return clients[id];
	}
	
	public String getHostnameById(int id){
		return hostKeyMapping[id];
	}
	
	public int getNumOfHosts(){
		return clients.length;
	}
	
	public void flushTables(String basedir){
		if (clients[0] == null)
			startClients();
		for (int i = 0; i < clients.length; i++) {
			if (clients[i] == null)
				LOG.error("null client for "+ hostKeyMapping[i]);
			else
				clients[i].writeToLocalFileSystem(basedir);
		}
		stopClients();
	}
	
	public void initialize(){
		if (clients[0] == null)
			startClients();
		for (int i = 0; i < clients.length; i++) {
			if (clients[i] == null)
				LOG.error("null client for "+ hostKeyMapping[i]);
			else
				clients[i].initialize();
		}
		stopClients();
	}
	public static void main(String[] args) {
		String s = "<dcterms:audience>National Library of Ireland</dcterms:audience>";
		System.out.println(s.hashCode());
	}
}
