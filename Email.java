import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;


public class Email {
 public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature  ");
      System.exit(-1);
    }
	////////////////////////////
	Configuration conf = new Configuration();
    Job job = new Job(conf, "email");
	job.setJarByClass(Email.class);
	job.setMapperClass(EmailMapper.class);
	job.setReducerClass(EmailReducer.class);
	job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	//job.setNumReduceTasks(1);
	/*job.setPartitionerClass(ActualKeyPartitioner.class);
	job.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
	job.setSortComparatorClass(CompositeKeyComparator.class);*/
	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	boolean result = job.waitForCompletion(true);
	System.exit(result ? 0 : 1);

	/*
	
    JobConf conf = new JobConf(Trader.class);
    conf.setJobName("Trader");
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	conf.setMapOutputKeyClass(CompositeKey.class);
    conf.setMapperClass(TraderMapper.class);
    conf.setReducerClass(TraderReducer.class);
	conf.setPartitionerClass(ActualKeyPartitioner.class);
	conf.setGroupingComparatorClass(ActualKeyGroupingComparator.class);
	conf.setSortComparatorClass(CompositeKeyComparator.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
	conf.setMapOutputValueClass(TransactionInfo.class);
    JobClient.runJob(conf);*/
  }
}