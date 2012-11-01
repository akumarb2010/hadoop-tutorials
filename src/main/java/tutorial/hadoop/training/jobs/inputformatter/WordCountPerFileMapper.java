package tutorial.hadoop.training.jobs.inputformatter;

import java.util.StringTokenizer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountPerFileMapper extends
		Mapper<Text, BytesWritable, WordCountPerFileKey, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);

	@Override
	protected void map(Text key, BytesWritable value, Context context)
			throws java.io.IOException, InterruptedException {
		String fileName = key.toString();
		String content = new String(value.getBytes());
		StringTokenizer strTock = new StringTokenizer(content, " ");
		while (strTock.hasMoreTokens()) {
			WordCountPerFileKey wcKey = new WordCountPerFileKey();
			wcKey.setFileName(fileName);
			wcKey.setWord(strTock.nextToken());
			context.write(wcKey, one);
		}
	};

}
