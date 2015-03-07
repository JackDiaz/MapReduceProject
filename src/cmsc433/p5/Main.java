package cmsc433.p5;


import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
	
	/** Temporary directory used to store output from CodeCompareMR
	 * to feed as input into ValueSortMR */
	protected static final String TEMP_DIR = "temp";

	/**
	 * Entry-point for the program. Should accept three command line arguments:
	 * <ul>
	 * <li><code>source </code>Path to source code to evaluate</li>
	 * <li><code>in </code>Path to directory containing posts to match</li>
	 * <li><code>out </code>Path to output directory to store final result</li>
	 * </ul>
	 * The final output should be a list of post titles followed by their
	 * relevance count sorted in descending order.
	 */
	public static void main(String[] args) throws Exception {
		// Create configuration and parse arguments
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: Main <source> <in> <out>");
			System.exit(2);
		}
		// At this point "otherArgs" should contain {<source>, <in>, <out>}
		
		// delete the last temp file used
		deleteDirectory(new File(TEMP_DIR));
		// delete the last output file used
		deleteDirectory(new File(otherArgs[2]));
		
		boolean success = CodeCompareMR.evaluate(new Job(conf, "codecompare"), otherArgs[0], otherArgs[1], TEMP_DIR);
		
		if (success)
			success = ValueSortMR.sort(new Job(conf, "sort"), TEMP_DIR, otherArgs[2]);
		
		System.exit(success ? 0 : 1);
	}
	
	// deletes the directory specified, borrowed from stack overflow
	public static boolean deleteDirectory(File directory) {
	    if(directory.exists()){
	        File[] files = directory.listFiles();
	        if(null!=files){
	            for(int i=0; i<files.length; i++) {
	                if(files[i].isDirectory()) {
	                    deleteDirectory(files[i]);
	                }
	                else {
	                    files[i].delete();
	                }
	            }
	        }
	    }
	    return(directory.delete());
	}

}
