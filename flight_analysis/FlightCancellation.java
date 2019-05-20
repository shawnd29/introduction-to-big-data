import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter.DEFAULT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class FlightCancellationMapper extends Mapper<Object, Text, Text, IntWritable>{

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String file = value.toString();
		String[] entry = file.split(",");
		
		String cancel_flag = entry[21];
		String cancel_code = entry[22];
		
		if(!cancel_flag.equalsIgnoreCase("Cancelled") && !cancel_flag.equalsIgnoreCase("NA")
				&& cancel_flag.equals("1") && !cancel_code.equalsIgnoreCase("CancellationCode")
				&& !cancel_code.equalsIgnoreCase("NA")) {
			
			context.write(new Text(cancel_code), new IntWritable(1));
			
		}
		
	}
	
}

class FlightCancellationReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	public ArrayList<customCancel> listCancelMap = new ArrayList<customCancel>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int total_count = 0;
		int cancel_count = 0;
		
		Iterator<IntWritable> iterator = values.iterator();
		
		while(iterator.hasNext()) {
			
			cancel_count = cancel_count + iterator.next().get();
			total_count++;
			
		}
		
		listCancelMap.add(new customCancel(key.toString(), cancel_count));
		
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		Collections.sort(listCancelMap, new Comparator<customCancel>() {

			@Override
			public int compare(customCancel c1, customCancel c2) {
				return c2.can_count.compareTo(c1.can_count);
				
			}
		});
		if(listCancelMap.size() > 0) {
			customCancel result_code = listCancelMap.get(0);
		
		switch(result_code.cancel_code){
			
			case "A":
				context.write(new Text(result_code.cancel_code+" : Carrier"), new IntWritable(result_code.can_count));
				break;
			
			case "B":
				context.write(new Text(result_code.cancel_code+" : Weather"), new IntWritable(result_code.can_count));
				break;
			
			case "C":
				context.write(new Text(result_code.cancel_code+" : NAS"), new IntWritable(result_code.can_count));
				break;
				
			case "D":
				context.write(new Text(result_code.cancel_code+" : Security"), new IntWritable(result_code.can_count));
				break;
				
			default:
				context.write(new Text(result_code.cancel_code+" : No Common Reason"), new IntWritable(result_code.can_count));
			
			
		}
		}
		else {
			context.write(new Text("No cancellation data available"), new IntWritable());
		}
		
	}

	
	class customCancel{
		
		String cancel_code;
		Integer can_count;
		public customCancel(String cancel_code, int can_count) {
			super();
			this.cancel_code = cancel_code;
			this.can_count = can_count;
		}
		
	}
	
}

public class FlightCancellation {
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		
		Configuration conf = new Configuration();
        Job job = new Job(conf, "FlightCancellation");
        job.setJarByClass(FlightCancellation.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(FlightCancellationMapper.class);
        job.setReducerClass(FlightCancellationReducer.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
