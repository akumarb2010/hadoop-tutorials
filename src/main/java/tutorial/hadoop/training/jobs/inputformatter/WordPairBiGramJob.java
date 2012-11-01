package tutorial.hadoop.training.jobs.inputformatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordPairBiGramJob implements Tool {
	
	enum badRecords
	{
		BADCOUNT
	}

	private Configuration conf;

	@Override
	public int run(String[] args) throws Exception {
		Job WordPairBiGramJob = new Job(getConf());
		WordPairBiGramJob.setJobName("Kelly WordPair Count");
		WordPairBiGramJob.setJarByClass(this.getClass());

		WordPairBiGramJob.setMapperClass(WordPairBiGramMapper.class);
		WordPairBiGramJob.setMapOutputKeyClass(WordPairBiGramKey.class);
		WordPairBiGramJob.setMapOutputValueClass(LongWritable.class);

		WordPairBiGramJob.setReducerClass(WordPairBiGramReducer.class);
		WordPairBiGramJob.setOutputKeyClass(WordPairBiGramKey.class);
		WordPairBiGramJob.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(WordPairBiGramJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(WordPairBiGramJob, new Path(args[1]));

		return WordPairBiGramJob.waitForCompletion(true) == true ? 0 : -1;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordPairBiGramJob(), args);
	}

}
