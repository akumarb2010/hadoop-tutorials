package tutorial.hadoop.training.jobs.inputformatter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordPairBiGramReducer extends
		Reducer<WordPairBiGramKey, LongWritable, WordPairBiGramKey, LongWritable> {

	@Override
	protected void reduce(WordPairBiGramKey key,
			java.lang.Iterable<LongWritable> values, Context context)
			throws java.io.IOException, InterruptedException {
		long sum = 0;
		while (values.iterator().hasNext()) {
			sum += values.iterator().next().get();
		}
		context.write(key, new LongWritable(sum));
	};

}
