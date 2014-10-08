import java.io.IOException; 
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer; 

	
public class EmailReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{ 
		
		int sum = 0;	
		for (IntWritable value : values){
			sum += value.get();
		}	
		String keyString = key.toString();
		String[] subString = keyString.split(Pattern.quote("$"));
		if(subString != null && subString.length == 2){
			Text newKey = new Text("From: " + subString[0] + "   To: " + subString[1]);
			context.write(newKey, new IntWritable(sum));
		}
					
	}
}