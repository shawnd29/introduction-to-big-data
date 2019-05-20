// This program is used to find the missing cards from a set of random regular cards
//Done by Shawn D'Souza - srd59

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;

public class cards extends Configured implements Tool {

    public static class cardsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text ckey = new Text();
        private IntWritable cval = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int i = 0;

            String[] two = { "HEARTS", "DIAMONDS", "SPADES", "CLUBS" };
            while (i < 4) {

                ckey.set(two[i]);
                cval.set(0);
                context.write(ckey, cval);
                i++;

            }

            String singleLine = value.toString();
            String[] card = singleLine.split("\t");
            ckey.set(card[0]);
            cval.set(Integer.parseInt(card[1]));

            context.write(ckey, cval);

        }
    }

    public static class cardsReducer extends Reducer<Text, IntWritable, Text, Text> {
        Text missedcard = new Text();

        public void reduce(Text token, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<Integer> cards = new ArrayList<Integer>();

            int total = 0;
            int number = 0;

            for (IntWritable val : values) {
                total += val.get();
                number = val.get();
                cards.add(number);
            }
            StringBuilder s1 = new StringBuilder();
            if (total <= 90) {
                for (int j = 1; j <= 13; j++) {
                    if (!cards.contains(j))
                        s1.append(j).append("  ");
                }
                missedcard.set(s1.substring(0, s1.length() - 1));
            } else {
                missedcard.set("None of the cards are missing");
            }
            context.write(token, missedcard);
        }
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "cards");
        job.setJarByClass(getClass());
        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(cardsMapper.class);
        job.setReducerClass(cardsReducer.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new cards(), args);
        System.exit(result);
    }

}
