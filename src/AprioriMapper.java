import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Enridestroy
 * on va recuperer les items dans chaque transaction et les envoyer au reducer.
 * lui, il va agreger toutes les transactions et donc va nous donner la frequence de chaque item.
 */
public class AprioriMapper extends Mapper<Text, MRTransactionArrayWritable, Text, MRTransactionArrayWritable>{
	/**
	 * il faut encore remplacer les Text par ArrayWritable. comme on a fait ailleurs.
	 */
	public void map(Text key, MRTransactionArrayWritable value, Context context) throws IOException, InterruptedException {
		//on va verifier que le motif est satifait dans toutes les transactions ?
		MRTransaction[] trx = ((MRTransaction[])value.toArray());//on pourrait utiliser Arrays.asList() puis des remove().
		ArrayList<MRTransaction> ss = new ArrayList<>();
		for(MRTransaction tr : trx){
			if(tr.contains((Text[])TextArrayWritable.fromText(key).toArray())){
				ss.add(tr);
			}
		}
		
		//si rien ne matche, ca sert a rien de l'envoyer...
		if(ss.size()>0){
			//on ne compte pas ici, meme si on pourrait,
			context.write(key, new MRTransactionArrayWritable(ss.toArray(new MRTransaction[ss.size()])));
		}
	}

}
