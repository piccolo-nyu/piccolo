package edu.nyu.cs.piccolo.kernel;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.swing.plaf.basic.BasicSplitPaneUI.KeyboardEndHandler;
import javax.swing.table.TableStringConverter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair;

public abstract class Kernel<KEYIN, VALUEIN, TABLEKEY, TABLEVALUE> {
	
	private PiccoloTable<TABLEKEY, TABLEVALUE> table = null;
	private String name; 

	public Kernel(String kernelName){
		table = kernelTable();
		name = kernelName;
	}
	
	// function to be applied for every input 
	public abstract TablePair<TABLEKEY, TABLEVALUE> kernelfunction(KEYIN key, VALUEIN value);
	public abstract PiccoloTable<TABLEKEY, TABLEVALUE> kernelTable();
	
	public abstract void writeOutKernelTable(FileSystem fs, Path path) throws IOException;
	
	public String getName(){
		return name;
	}
	public PiccoloTable<TABLEKEY, TABLEVALUE> getTable(){
		return table;
	}
}
