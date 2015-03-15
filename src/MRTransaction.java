import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * @author Enridestroy
 *
 */
public class MRTransaction implements Writable{
	//
	private TextArrayWritable items;// = new ArrayList<>();
	
	public MRTransaction(Text[] trx) {
		//this.items = new ArrayList<>();//
		//System.out.println("MRLISTTTTT");
		//Collections.addAll(this.items, trx);
		this.items = new TextArrayWritable(trx);
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public boolean contains(String key){
		Text[] oo = (Text[])items.toArray();
		List l = Arrays.asList(oo);
		char[] cc = key.toCharArray();
		for(Character c : cc){
			if(!l.contains(c.toString())){
				return false;
			}
		}
		return true;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public boolean contains(ArrayList<String> key){
		Text[] oo = (Text[])items.toArray();
		List l = Arrays.asList(oo);
		for(String s : key){
			if(!l.contains(s)){
				return false;
			}
		}
		return false;
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public boolean contains(Text[] key){
		Text[] oo = (Text[])items.toArray();
		List l = Arrays.asList(oo);
		for(Text s : key){
			if(!l.contains(s.toString())){
				return false;
			}
		}
		return false;
	}
	
	/**
	 * 
	 */
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.items.readFields(arg0);
	}

	/**
	 * 
	 */
	@Override
	public void write(DataOutput arg0) throws IOException {
		this.items.write(arg0);
//		TextArrayWritable tt = new TextArrayWritable();
//		String[] ss = this.items.toArray(new String[this.items.size()]);
//		Text[] casted = new Text[ss.length];
//		for(int i=0;i<ss.length;i++){
//			casted[i] = new Text(ss[i]);
//		}
//		tt.write(arg0);
	}

}
