package cmsc433.p5;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cmsc433.p5.CodeCompareMR.HashReducer;
import cmsc433.p5.CodeCompareMR.TokenizerMapper;

/*
 * Jack Diaz
 * 111499298
 */

/**
 * This class uses Hadoop to take an input file where each line consists
 * of a string, a tab, and then an integer, and outputs a list where the keys
 * are the strings and the values are the integers, except the output should now be sorted by the
 * natural ordering of the values, the integers, from greatest to least, and not the keys.
 */
public class ValueSortMR {

	/** Minimum <code>int</code> value for a pair to be included in the output.
	 * Pairs with an <code>int</code> less than this value are omitted. */
	public static int CUTOFF = 1;


	public static class SwapMapper extends
	Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// Read lines using only tabs as the delimiter, as titles can contain spaces
			// (Both the string and integer are in the value field, not the key field.)
			StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			// TODO: IMPLEMENT CODE HERE~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			while(itr.hasMoreElements()){
				Text a = new Text(itr.nextToken());
				IntWritable b = new IntWritable(-1*Integer.parseInt(itr.nextToken()));
				if(b.get() <= -1*CUTOFF){
					context.write(b, a);
				}
			}
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		}
	}

	public static class SwapReducer extends
	Reducer<IntWritable, Text, Text, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// TODO: IMPLEMENT CODE HERE~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			List<String> toSort = new ArrayList<String>();
			for(Text v : values){
				toSort.add(v.toString());
			}
			Collections.sort(toSort);
			for(String s : toSort){
				context.write(new Text(s), new IntWritable(key.get()*-1));
			}
			//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		}
	}


	/**
	 * This method performs value-based sorting on the given input by
	 * configuring the job as appropriate and using Hadoop.
	 * @param job Job created for this function
	 * @param input String representing location of input directory
	 * @param output String representing location of output directory
	 * @return True if successful, false otherwise
	 * @throws Exception
	 */
	public static boolean sort(Job job, String input, String output) throws Exception {
		job.setJarByClass(ValueSortMR.class);

		// TODO: IMPLEMENT CODE HERE~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		job.setMapperClass(SwapMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		

		job.setReducerClass(SwapReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}
}
