package edu.nyu.cs.piccolo.kernel;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

public abstract class PiccoloTable<KEY, VALUE> {

	
	private Hashtable<KEY, VALUE> table= new Hashtable<KEY, VALUE>();
	
	public abstract VALUE accumulator(VALUE currentVal, VALUE newVal);
	public abstract String tablePairToString(TablePair<KEY, VALUE> pair);
		
	//Defaults partitioner = hashPartitioner
	//returns worker id
	//params: key
	public int partitionFunction(KEY key, VALUE val, int numOfPartitions){
		return (key.hashCode() & Integer.MAX_VALUE) % numOfPartitions;
	}
	
	protected synchronized void put(KEY key, VALUE val)
	{
		if (table.containsKey(key))
			table.put(key, accumulator(table.get(key), val));
		else
			table.put(key, val);
	}
	
	public VALUE get(KEY key) {
		return table.get(key);
	}
	
	/*public ArrayList<TablePair<KEY, VALUE>> getIterator() {
		ArrayList<TablePair<KEY, VALUE>> arrl = new ArrayList<TablePair<KEY,VALUE>>();
		for (Iterator iterator = table.keySet().iterator(); iterator.hasNext();) {
			KEY k = (KEY) iterator.next();
			arrl.add(new TablePair<KEY, VALUE>(k, table.get(k)));
		}
		return arrl;
	}
	
	public Set<KEY> getKeySet(){
		return table.keySet();
	}*/
	
	protected Set<Entry<KEY, VALUE>> getEntrySet(){
		return table.entrySet();
	}
	
	public int getSize(){
		return table.size();
	}
	private void destroy(){
		table = null;
	}
	
	public static class TablePair<KEY, VALUE>
	{
		KEY key; 
		VALUE value; 
		
		public TablePair(KEY k, VALUE v){
			this.key = k;
			this.value = v;
		}
		
		public KEY getKey() {
			return key;
		}
		public VALUE getValue() {
			return value;
		}
		
	}
}
