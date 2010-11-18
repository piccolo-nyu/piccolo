package edu.nyu.cs.piccolo.kernel;

import java.util.Hashtable;
import java.util.Map.Entry;

public abstract class PiccoloTable<KEY, VALUE> {

	
	private Hashtable<KEY, VALUE> table= new Hashtable<KEY, VALUE>();
	
	public abstract VALUE accumulator(VALUE currentVal, VALUE newVal);
	
	//Defaults partitioner = hashPartitioner
	//returns worker id
	//params: key
	public int partitionFunction(KEY key, VALUE val, int numOfPartitions){
		return (key.hashCode() & Integer.MAX_VALUE) % numOfPartitions;
	}
	
	public void put(KEY key, VALUE val)
	{
		
		if (table.containsKey(key))
			table.put(key, accumulator(table.get(key), val));
		else
			table.put(key, val);
		
	}
	
	public VALUE get(KEY key) {
		return table.get(key);
	}
	
	public Entry<KEY,VALUE>[] getIterator() {
		return (Entry<KEY, VALUE>[]) table.entrySet().toArray(); 
	}
	
	private Hashtable<KEY, VALUE> getTable() {
		return table;
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
