package edu.nyu.cs.piccolo.kernel;

import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public abstract class KernelTableDefinition<TABLEKEY extends Writable, TABLEVALUE extends Writable> extends KernelBase {
	
	private PiccoloTable<TABLEKEY, TABLEVALUE> table;

	public KernelTableDefinition(String kernelName) {
		super(kernelName);
		table = kernelTable();
	}
	
	public abstract PiccoloTable<TABLEKEY, TABLEVALUE> kernelTable();
	//public abstract void writeOutKernelTable(FileSystem fs, Path path) throws IOException;
	
	public void addToTable(TABLEKEY  key, TABLEVALUE val){
		table.put(key, val);
	}
	public Set<Entry<TABLEKEY, TABLEVALUE>> getTableEntrySet(){
		return table.getEntrySet();
	}
}
