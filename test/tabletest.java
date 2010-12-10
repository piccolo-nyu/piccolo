package edu.nyu.cs.piccolo.test;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

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

		@Override
		public String tablePairToString(edu.nyu.cs.piccolo.kernel.PiccoloTable.TablePair<String, Integer> pair) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}

	public static void main(String[] args) {
		MyTable tabl = new MyTable();
		
		/*tabl.put("a", 4);
		System.out.println("a" + tabl.get("a"));
		tabl.put("a", 2);
		System.out.println("a" + tabl.get("a"));
		tabl.put("a", 9);
		System.out.println("a" + tabl.get("a"));
		tabl.put("b", 29);
		System.out.println("b" + tabl.get("b"));
		*/
		Hashtable<Text, Integer> tbl = new Hashtable<Text, Integer>();
		tbl.put(new Text("aa"), 1);
		tbl.put(new Text("aa"), 1);
		tbl.put(new Text("aa"), 4);
		tbl.put(new Text("bb"), 9);
		System.out.println( tbl.toString());
		Set<Text> s = tbl.keySet();
		Iterator<Text> itr = s.iterator(); 
		while(itr.hasNext())
		{
			System.out.println(itr.next().toString());
		}
	}
}
