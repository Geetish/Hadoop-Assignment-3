import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmailMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	static boolean flag = false;
	static String from = "";
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		//pattern = Pattern.compile(EMAIL_PATTERN);
		
		// Check if there is a Message-ID
		if(line.indexOf("Message-ID")!=-1){
			flag = true;
		}	

		//if the line contains "Message-ID", check lines after the line to find the From
		if(flag == true){
				
			if((line.indexOf("From:") != -1) && (line.indexOf("@",line.indexOf("From:")+1)!=-1)){
				from = line.substring(line.indexOf(":")+1,line.indexOf("@",line.indexOf("From:")+1));
				
			}
			else if((line.indexOf("To:") != -1) && (line.indexOf("@",line.indexOf("To:")+1)!=-1) && from.length()>0){		
					String to = line.substring(line.indexOf(":")+1).trim();
					if(to!=null){
						String[] to_email_addresses = to.split(",");
						for(String toemail :to_email_addresses){
							String[] name=toemail.split("@");
							if(name!=null && name.length==2){
								Text emailKey = new Text(from + "$" + name[0].trim());
								IntWritable one = new IntWritable(1); 
								context.write(emailKey, one);
							}
						}
					}
					//flag = false;
					//from = "";
			}
			
			else if((line.indexOf("Subject:") == -1)&&from.length()!=0){		
				//String to = line.substring(line.indexOf(":")+1).trim();
				//if(to!=null){
					String[] to_email_addresses = line.split(",");
					for(String toemail :to_email_addresses){
						String[] name=toemail.split("@");
						if(name!=null && name.length==2){
							Text emailKey = new Text(from + "$" + name[0].trim());
							IntWritable one = new IntWritable(1); 
							context.write(emailKey, one);
						}
					}
				//}
				//flag = false;
				//from = "";
			}
			
			else if((line.indexOf("Subject:") != -1)&&from.length()!=0){		
				flag = false;
				from = "";
			}
		}
			
	}
}

