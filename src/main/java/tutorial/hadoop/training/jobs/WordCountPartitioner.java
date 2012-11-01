package tutorial.hadoop.training.jobs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<Text, LongWritable> {

	@Override
	public int getPartition(Text text, LongWritable lw, int noOfReducers) {
		return (text.toString().charAt(0) - 'a')%noOfReducers;
	}
}
