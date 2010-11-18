package edu.nyu.cs.piccolo.worker;

import java.util.Hashtable;

import edu.nyu.cs.piccolo.kernel.PiccoloTable;

//not used currently 
public class PiccoloWorker {

	private Hashtable<String, PiccoloTable> tables = new Hashtable<String, PiccoloTable>(); 
	
	private PiccoloTable getTable(String tableName)
	{
		return tables.get(tableName);
	}
	
	private void addTable(String name, PiccoloTable table)
	{
		tables.put(name, table);
	}
	
	protected void put(String tableName, Object key, Object value)
	{
		tables.get(tableName).put(key, value);
	}
	
}
