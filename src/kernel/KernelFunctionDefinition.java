package edu.nyu.cs.piccolo.kernel;

import org.apache.hadoop.io.Writable;

import edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair;

public abstract class KernelFunctionDefinition<MAPKEYIN, MAPVALUEIN, TABLEKEY extends Writable, TABLEVALUE extends Writable> extends KernelBase<MAPKEYIN, MAPVALUEIN, TABLEKEY, TABLEVALUE>{
	
	public KernelFunctionDefinition(String kernelName) {
		super(kernelName);
	}

	public abstract TablePair<TABLEKEY, TABLEVALUE> kernelfunction(MAPKEYIN key, MAPVALUEIN value);
	public abstract int hash(TABLEKEY key);
	
}
