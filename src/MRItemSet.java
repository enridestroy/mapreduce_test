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
public class MRItemSet implements Writable{
	private ArrayList<String> items = new ArrayList<>();
	
	public MRItemSet() {
		// TODO Auto-generated constructor stub
//		this.requestId = new Text();
//        this.requestType = new Text();
//        this.timestamp = new LongWritable();
	}
	
	public MRItemSet(String asString){
		
	}
	
	public MRItemSet(ArrayList<String> asList){
		
	}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //requestId.write(dataOutput);
        //requestType.write(dataOutput);
        //timestamp.write(dataOutput);
        
        StringBuilder sb = new StringBuilder();
        for(String s : this.items){
        	sb.append(s);
        }
        new Text(sb.toString()).write(dataOutput);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //requestId.readFields(dataInput);
        //requestType.readFields(dataInput);
        //timestamp.readFields(dataInput);
        
        Text t = new Text();
        t.readFields(dataInput);
        
        for(Character c : t.toString().toCharArray()){
        	//this.items.add();
        }
        		
    }
    
    /**
     * getters
     */

    @Override
    public int hashCode() {
        // This is used by HashPartitioner, so implement it as per need
        // this one shall hash based on request id
        return items.hashCode();
    }



}
