import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

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
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}
