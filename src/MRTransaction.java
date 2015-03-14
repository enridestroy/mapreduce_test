import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
	private ArrayList<String> items = new ArrayList<>();
	
	public MRTransaction(String[] trx) {
		this.items = (ArrayList<String>)Arrays.asList(trx);
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 */
	public boolean contains(String key){
		char[] cc = key.toCharArray();
		for(Character c : cc){
			if(!items.contains(c.toString())){
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
		for(String s : key){
			if(!this.items.contains(s)){
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
		for(Text s : key){
			if(!this.items.contains(s.toString())){
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
		ArrayWritable tt = new ArrayWritable(Text.class);
		tt.readFields(arg0);
		for(Text t : (Text[])tt.toArray()){
			this.items.add(t.toString());
		}
	}

	/**
	 * 
	 */
	@Override
	public void write(DataOutput arg0) throws IOException {
		ArrayWritable tt = new ArrayWritable(Text.class);
		String[] ss = this.items.toArray(new String[this.items.size()]);
		Text[] casted = new Text[ss.length];
		for(int i=0;i<ss.length;i++){
			casted[i] = new Text(ss[i]);
		}
		tt.write(arg0);
	}

}
