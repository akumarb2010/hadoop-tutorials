package tutorial.hadoop.training.jobs.inputformatter;

import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordPairBiGramMapper extends
		Mapper<LongWritable, Text, WordPairBiGramKey, LongWritable> {

	private final static LongWritable one = new LongWritable(1);
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString(), " ");
		String word1 = st.nextToken();
		String word2 = null;
		while(st.hasMoreTokens())
		{
			word2 = st.nextToken();
			WordPairBiGramKey key1 = new WordPairBiGramKey();
			key1.setWord1(word1);
			key1.setWord2(word2);
			context.write(key1, one);
			word1 = word2;
		}
	};

	
}
