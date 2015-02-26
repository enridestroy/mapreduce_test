import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.json.parsers.JSONParser;
import com.json.parsers.JsonParserFactory;
import com.json.exceptions.JSONParsingException;


public class YelpMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		/**
		 * on va compter les reviews et sortir combien de review par users
		 */
		String line = value.toString();
		//String year = line.substring(15, 19);
		//String user = value.toString();
		
		JsonParserFactory factory = JsonParserFactory.getInstance();
        JSONParser parser = factory.newJsonParser();
        parser.setValidating(false);
        
        Map<String, Object> jsonMap = null;
        
        try{
        	jsonMap = parser.parseJson(line);
        }
        catch(JSONParsingException e){
        	System.out.println("erreur du parsing json.");
        }
                
        if(jsonMap!=null && jsonMap.get("type")!=null && jsonMap.get("type").equals("review")){
        	String user = (String) jsonMap.get("user_id");
        	context.write(new Text(user), new IntWritable(1));//on ajoute un item au user en question
        }		
	}
}
