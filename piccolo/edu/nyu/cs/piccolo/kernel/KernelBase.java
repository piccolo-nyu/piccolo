package edu.nyu.cs.piccolo.kernel;

public abstract class KernelBase<MAPKEYIN, MAPVALUEIN, TABLEKEY, TABLEVALUE> {
	
	
	private String name; 
	
	public KernelBase(String kernelName){
		name = kernelName;
	}
	
		
	public String getName(){
		return name;
	}
}