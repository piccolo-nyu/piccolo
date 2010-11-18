package edu.nyu.cs.piccolo.test;

import java.util.Set;
import java.util.Map.Entry;

import edu.nyu.cs.piccolo.kernel.PiccoloTable;


public class tabletest {
	
	public static class MyTable extends PiccoloTable<String, Integer>
	{

		@Override
		public Integer accumulator(Integer val1, Integer val2) {
			if (val2>val1)
				return val2;
			else 
				return val1;
			//return val1+val2;
		}
		
	}

	public static void main(String[] args) {
		MyTable tabl = new MyTable();
		
		tabl.put("a", 4);
		System.out.println("a" + tabl.get("a"));
		tabl.put("a", 2);
		System.out.println("a" + tabl.get("a"));
		tabl.put("a", 9);
		System.out.println("a" + tabl.get("a"));
		tabl.put("b", 29);
		System.out.println("b" + tabl.get("b"));
		
		Entry<String, Integer>[] x = tabl.getIterator();
		System.out.println("xsize" + x.length);
		Entry<String, Integer> entrytidel = x[1]; 
		
		System.out.println("entrytidel " + entrytidel.toString());
		
		System.out.println("a" + tabl.get("a"));
		
	}
}
