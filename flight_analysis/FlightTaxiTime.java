import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;



class FlightTaxiTimeMapper extends Mapper<Object, Text, Text, DoubleWritable>{

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String file = value.toString();
		String[] entry = file.split(",");
		
		String origin = entry[16];
        String dest = entry[17];
        String taxiInTime = entry[19];
        String taxiOutTime = entry[20];
        
        if(!origin.equalsIgnoreCase("Origin") && !origin.equalsIgnoreCase("NA")
        		&& !dest.equalsIgnoreCase("Dest") && !dest.equalsIgnoreCase("NA")
        		&& !taxiInTime.equalsIgnoreCase("TaxiIn") && !taxiInTime.equalsIgnoreCase("NA")
        		&& !taxiOutTime.equalsIgnoreCase("TaxiOut") && !taxiOutTime.equalsIgnoreCase("NA")) {
        	
        	context.write(new Text(origin), new DoubleWritable(Integer.parseInt(taxiOutTime)));
        	context.write(new Text(dest), new DoubleWritable(Integer.parseInt(taxiInTime)));
        	
        }
		
	}
	
}
	class FlightTaxiTimeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

		public ArrayList<customTaxiTime> listTaxiMap = new ArrayList<customTaxiTime>();
		
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			int total_count = 0;
			double taxiTimeVal = 0;
			
			Iterator<DoubleWritable> iterator = values.iterator();
			
			while(iterator.hasNext()) {
				
				taxiTimeVal = taxiTimeVal + iterator.next().get();
				total_count++;
				
			}
			
			double avgTaxiTime = (double) taxiTimeVal / (double) total_count;
			
			listTaxiMap.add(new customTaxiTime(key.toString(), avgTaxiTime));
			
		}
		
		class customTaxiTime{
			
			String airport;
			Double avg_taxi_time;
			
			public customTaxiTime(String airport, Double avg_taxi_time) {
				super();
				this.airport = airport;
				this.avg_taxi_time = avg_taxi_time;
			}
		
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			
			Collections.sort(listTaxiMap, new Comparator<customTaxiTime>() {

				@Override
				public int compare(customTaxiTime c1, customTaxiTime c2) {
					
					return c2.avg_taxi_time.compareTo(c1.avg_taxi_time);
					
				}
			});
			
			if(listTaxiMap.size() > 0) {
			
				context.write(new Text("Top 3 airports with longest taxi time"),new DoubleWritable(0.0));
				
				for(int i=0;i<3;i++) {
					customTaxiTime c = listTaxiMap.get(i);
					context.write(new Text(c.airport),new DoubleWritable(c.avg_taxi_time));
				}
				
				context.write(new Text("Top 3 airports with shortest taxi time"),new DoubleWritable(0.0));
				
				for(int i=(listTaxiMap.size()-1) ; i > (listTaxiMap.size()-4) ; i--) {
					customTaxiTime c = listTaxiMap.get(i);
					context.write(new Text(c.airport),new DoubleWritable(c.avg_taxi_time));
				}
			
			}
			else {
				context.write(new Text("No Taxi Time data available"), new DoubleWritable());
			}
			
		}

		
	}
	
	
	public class FlightTaxiTime {
		
		public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
			
			Configuration conf = new Configuration();
	        Job job = new Job(conf, "FlightTaxiTime");
	        job.setJarByClass(FlightTaxiTime.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(DoubleWritable.class);
	        job.setMapperClass(FlightTaxiTimeMapper.class);
	        job.setReducerClass(FlightTaxiTimeReducer.class);
	        
	        FileInputFormat.setInputPaths(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		}
	
	}
