/**
 * 
 */
package uk.bl.dpt.hadoopinputstats;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Class to generate stats about input files
 * (Files expected to be listed in a text file in HDFS)
 * @author wpalmer
 */
public class HadoopInputStats extends Configured implements Tool {

	/**
	 * Collect the size of each file
	 * @author wpalmer
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

		private static FileSystem gFS = null;
		
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			super.configure(job);
			try {
				gFS = FileSystem.get(job);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
						throws IOException {

			// load file from HDFS and get file size
			Path p = new Path(value.toString());
			try {
				FileStatus fileStatus = gFS.getFileStatus(p);
				if(fileStatus!=null) {
					collector.collect(new Text("File"), new LongWritable(fileStatus.getLen()));
				} else {
					collector.collect(new Text("Null"), new LongWritable(0));
				} 
			} catch(Exception e) {
				collector.collect(new Text("Exception"), new LongWritable(0));
			}

		}

	}
	
	/**
	 * Reduce to get file count and total size (and error count)
	 * @author wpalmer
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> { 

		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
						throws IOException {

			long count = 0;
			long totalSize = 0;

			while(values.hasNext()) {
				count++;
				totalSize += values.next().get();
			}

			collector.collect(new Text(key+": "+count), new LongWritable(totalSize));

		}

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		JobConf conf = new JobConf(HadoopInputStats.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path("HadoopInputStats"));
		
		//set the mapper to this class' mapper
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		//this input format should split the input by one line per map by default.
		conf.setInputFormat(NLineInputFormat.class);
		conf.setInt("mapred.line.input.format.linespermap", 5000);
		
		//sets how the output is written cf. OutputFormat
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		
		//we only want one reduce task
		conf.setNumReduceTasks(1);
		
		JobClient.runJob(conf);
		
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			ToolRunner.run(new HadoopInputStats(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
