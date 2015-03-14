import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * @author Enridestroy
 *
 */
public class SuffixCandANDTrx implements Writable{
	private ArrayList<Text> suffixe = new ArrayList<>();
	private ArrayList<MRTransaction> trx = new ArrayList<>();
	
	public ArrayList<Text> getSuffix(){
		return this.suffixe;
	}
	
	public SuffixCandANDTrx(ArrayList<Text> s, ArrayList<MRTransaction> tr) {
		this.suffixe = s;
		this.trx = tr;
	}
	
	/**
	 * 
	 * @param arg0
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		ArrayWritable tt = new ArrayWritable(Text.class);
		tt.readFields(arg0);
		for(Text t : (Text[])tt.toArray()){
			this.suffixe.add(t);
		}
		
		ArrayWritable ttt = new ArrayWritable(MRTransaction.class);
		ttt.readFields(arg0);
		for(MRTransaction t : (MRTransaction[])ttt.toArray()){
			this.trx.add(t);
		}
//		
//		for(Text t : this.suffixe)
//			t.readFields(arg0);
//		
//		for(MRTransaction tx : this.trx)
//			tx.readFields(arg0);
	}

	/**
	 * 
	 * @param arg0
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
//		for(Text t : this.suffixe)
//			t.write(arg0);
//		
//		for(MRTransaction tx : this.trx)
//			tx.write(arg0);
//		
		
		ArrayWritable tt = new ArrayWritable(Text.class);
		String[] ss = this.suffixe.toArray(new String[this.suffixe.size()]);
		Text[] casted = new Text[ss.length];
		for(int i=0;i<ss.length;i++){
			casted[i] = new Text(ss[i]);
		}
		tt.write(arg0);
		
		MRTransaction[] sss = this.trx.toArray(new MRTransaction[this.trx.size()]);
		ArrayWritable ttt = new ArrayWritable(MRTransaction.class, sss);
		tt.write(arg0);
		
	}
	
}
