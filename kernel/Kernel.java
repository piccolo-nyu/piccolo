package edu.nyu.cs.piccolo.kernel;

import java.util.Map;
import java.util.Map.Entry;
import edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair;

public abstract class Kernel<KEYIN, VALUEIN, TABLEKEY, TABLEVALUE> {
	
	public Kernel(){
		table = kernelTable();
	}
	
	PiccoloTable<TABLEKEY, TABLEVALUE> table = null;
	
	// function to be applied for every input 
	public abstract TablePair<TABLEKEY, TABLEVALUE> kernelfunction(KEYIN key, VALUEIN value);
	public abstract PiccoloTable<TABLEKEY, TABLEVALUE> kernelTable();
	
	public String toString(){
		String rets =  this.getClass().getName() + "\nTable:\n";
		Entry<TABLEKEY, TABLEVALUE>[] arr =  table.getIterator();
		for (int i = 0; i < arr.length; i++) {
			rets += "key:" + arr[i].getKey() + "\tvalue:" + arr[i].getValue() + "\n";
		}
		return rets;
	}
	
}
