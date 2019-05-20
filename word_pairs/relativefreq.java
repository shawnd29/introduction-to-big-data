
/* This assisgnment is to find the relative frequency of a given word pair for a text dataset

Done By Shawn D'Souza - srd59

*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class relativefreq {

	public static class word_freqMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString().trim().toLowerCase().replaceAll("[^a-z _+]", "");

			StringTokenizer token = new StringTokenizer(line, " ");
			List<String> words = new ArrayList<String>();

			while (token.hasMoreElements()) {

				String wordAB = new String(token.nextToken());
				wordAB = wordAB.replaceAll("\\t", "");
				wordAB = wordAB.trim();
				if (wordAB.length() < 2)
					continue;
				words.add(wordAB);

			}

			for (int i = 1; i < words.size(); i++) {

				if (i == 1) {
					context.write(new Text("!" + " " + words.get(i - 1).trim()), new LongWritable(1));
					context.write(new Text(words.get(i - 1).trim() + " " + words.get(i).trim()), new LongWritable(1));
					context.write(new Text("!" + " " + words.get(i).trim()), new LongWritable(1));
				} else {
					context.write(new Text(words.get(i - 1).trim() + " " + words.get(i).trim()), new LongWritable(1));
					context.write(new Text("!" + " " + words.get(i).trim()), new LongWritable(1));
				}
			}

		}

	}

	public static class word_freqReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (LongWritable val : values) {
				count += val.get();
			}
			context.write(key, new LongWritable(count));
		}
	}

	public static class pair_freqMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString().trim();
			String word = line.split("\\t")[0];
			int count = (int) Integer.parseInt(line.split("\\t")[1]);
			context.write(new Text(word), new LongWritable(count));
		}

	}

	public static class pair_freqReducer extends Reducer<Text, LongWritable, Text, Text> {

		HashMap<String, Integer> times = new HashMap<String, Integer>();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			if (key.toString().trim().contains("!")) {

				for (LongWritable val : values) {

					int countA = (int) Integer.parseInt(val.toString().trim());
					String wordA = key.toString().trim().split(" ")[1];

					if (times.containsKey(wordA)) {
						continue;
					} else {
						times.put(wordA, countA);
					}

				}
				return;
			}

			for (LongWritable val : values) {

				String wordA = key.toString().trim().split(" ")[0];
				int countA = times.get(wordA);
				context.write(key, new Text(val + " " + countA));

			}
		}
	}

	public static class top_pairMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString().trim();
			String word = line.split("\\t")[0];
			int countAB = (int) Integer.parseInt(line.split("\\t")[1].split(" ")[0]);// count of AB
			int countA = (int) Integer.parseInt(line.split("\\t")[1].split(" ")[1]);// count of A
			context.write(new LongWritable(countAB), new Text(word + " " + countA));

		}

	}

	// selecting the top 1000 relative to AB's count
	public static class top_pairReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		static int top_count = 1000;// most count

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text val : values) {

				if (top_count > 0) {
					context.write(key, val);
					top_count = top_count - 1;
				} else {
					break;
				}

			}
		}
	}

	// For Key: f(B|A) Value: AB
	// calculate f(B|A)
	// Relative frequency of A and B is countAB/countA
	// count(A,B) is the number of times A and B co-occur in a document
	// count(A) the number of times A occurs with anything else
	public static class rel_freqMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// format of line: A+" "+B+"\\t+count of AB
			String line = value.toString().trim();

			try {

				String wordA = line.split("\\t")[1].split(" ")[0];// get word A
				String wordB = line.split("\\t")[1].split(" ")[1];// get word B
				float countAB = (float) Integer.parseInt(line.split("\\t")[0]);// get count of AB
				float countA = (float) Integer.parseInt(line.split("\\t")[1].split(" ")[2]) - countAB;// countA
				float rel_freq = 0;
				// calculate f(B|A)
				if (countA != 0) {
					rel_freq = countAB / countA;
				}

				context.write(new FloatWritable(rel_freq), new Text(wordA + " " + wordB));
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	// select the top 100 according to the f(B|A) value
	public static class rel_freqReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {

		static int top_n = 100;// Top K

		public void reduce(FloatWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text val : values) {
				if (top_n > 0) {
					context.write(key, val);
					top_n = top_n - 1;
				} else {
					break;
				}

			}
		}
	}

	// sort the top 1000
	public static class sum_writecomparator extends WritableComparator {

		protected sum_writecomparator() {
			super(LongWritable.class, true);
		}

		@SuppressWarnings("datatypes")
		@Override
		public int compare(WritableComparable pair1, WritableComparable pair2) {

			LongWritable pairF1 = (LongWritable) pair1;
			LongWritable pairF2 = (LongWritable) pair2;

			return -1 * pairF1.compareTo(pairF2);
		}

	}

	// sort the top 100
	public static class top_writableComparator extends WritableComparator {

		protected top_writableComparator() {
			super(FloatWritable.class, true);
		}

		@SuppressWarnings("datatypes")
		@Override
		public int compare(WritableComparable pair1, WritableComparable pair2) {

			FloatWritable pairF1 = (FloatWritable) pair1;
			FloatWritable pairF2 = (FloatWritable) pair2;

			return -1 * pairF1.compareTo(pairF2);
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job word_freq = new Job(conf, "Number of times A and B co-occur and number of times A occurs in the document");
		word_freq.setJarByClass(relativefreq.class);
		FileInputFormat.addInputPath(word_freq, new Path(args[0]));
		FileOutputFormat.setOutputPath(word_freq, new Path("TEMP1"));
		word_freq.setMapperClass(word_freqMapper.class);
		word_freq.setReducerClass(word_freqReducer.class);
		word_freq.setMapOutputKeyClass(Text.class);
		word_freq.setMapOutputValueClass(LongWritable.class);
		word_freq.setOutputKeyClass(Text.class);
		word_freq.setOutputValueClass(LongWritable.class);
		word_freq.waitForCompletion(true);

		Job pair_freq = new Job(conf, "Count of A and B");
		pair_freq.setJarByClass(relativefreq.class);
		FileInputFormat.addInputPath(pair_freq, new Path("TEMP1"));
		FileOutputFormat.setOutputPath(pair_freq, new Path("TEMP2"));
		pair_freq.setMapperClass(pair_freqMapper.class);
		pair_freq.setReducerClass(pair_freqReducer.class);
		pair_freq.setMapOutputKeyClass(Text.class);
		pair_freq.setMapOutputValueClass(LongWritable.class);
		pair_freq.setOutputKeyClass(Text.class);
		pair_freq.setOutputValueClass(Text.class);
		pair_freq.waitForCompletion(true);

		Job top_pair = new Job(conf, "Top 1000 of A and B");
		top_pair.setJarByClass(relativefreq.class);
		FileInputFormat.addInputPath(top_pair, new Path("TEMP2"));
		FileOutputFormat.setOutputPath(top_pair, new Path("TEMP3"));
		top_pair.setMapperClass(top_pairMapper.class);
		top_pair.setReducerClass(top_pairReducer.class);
		top_pair.setMapOutputKeyClass(LongWritable.class);
		top_pair.setMapOutputValueClass(Text.class);
		top_pair.setSortComparatorClass(sum_writecomparator.class);
		top_pair.setOutputKeyClass(LongWritable.class);
		top_pair.setOutputValueClass(Text.class);
		top_pair.waitForCompletion(true);

		Job rel_freq = new Job(conf, "Top 100 relative frequency of A and B");
		rel_freq.setJarByClass(relativefreq.class);
		FileInputFormat.addInputPath(rel_freq, new Path("TEMP3"));
		FileOutputFormat.setOutputPath(rel_freq, new Path(args[1]));
		rel_freq.setMapperClass(rel_freqMapper.class);
		rel_freq.setReducerClass(rel_freqReducer.class);
		rel_freq.setMapOutputKeyClass(FloatWritable.class);
		rel_freq.setMapOutputValueClass(Text.class);
		rel_freq.setSortComparatorClass(top_writableComparator.class);
		rel_freq.setOutputKeyClass(FloatWritable.class);
		rel_freq.setOutputValueClass(Text.class);
		System.exit(rel_freq.waitForCompletion(true) ? 0 : 1);

	}

}
