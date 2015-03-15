import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Enridestroy
 *	on lit un fichier qui contient quoi 
 * on doit lire un fichier avec les transactions c sur.
 * et quoi d'autre ?
 * 
 * ce niveau, il va prendre les cles et generer des nouveaux items.
 * 
 */
public class AprioriMapper2 extends Mapper<Text, MRTransactionArrayWritable, Text, SuffixCandANDTrx>{
	//
	public void map(Text key, MRTransactionArrayWritable value, Context context) throws IOException, InterruptedException {
		ArrayList<Text> ss = new ArrayList<>();
		ss.addAll(Arrays.asList(((Text[])TextArrayWritable.fromText(key).toArray())));//en attendant		
		MRTransaction[] trx = ((MRTransaction[])value.toArray());
		ArrayList<Text> minKey = (ArrayList<Text>)ss.subList(0, ss.size()-1);
		context.write(new TextArrayWritable(minKey.toArray(new Text[minKey.size()])).createText(), new SuffixCandANDTrx(ss, (ArrayList<MRTransaction>)Arrays.asList(trx)));
		//context.write(new Text(item), trx);
		//ArrayList<MRTransaction> tt = new ArrayList<>();
		//String line = value.toString();
		//String[] items = line.split(" ");
		//for(String item : items){
			//tt.add(item);
		//}
		//new SuffixCandANDTrx(ss, value);
	}

}
