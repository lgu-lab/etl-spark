package com.acme.book;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.demo.framework.AbstractJob;
import org.demo.framework.FileLoader;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class BookJob extends AbstractJob {
	
	/**
	 * Job name  (the 'application name' for the job)
	 */
	private final static String JOB_NAME = "BookJob";

	/**
	 * Job master (the 'master' configuration for the job e.g. 'local[4]' )
	 */
	private final static String JOB_MASTER_CONFIG = "local[4]";

	/**
	 * Input file path
	 */
	private final static String INPUT_FILE_PATH = "D:/TMP/csv-files/books.csv";
	
	/**
	 * Input file reader options
	 */
	private final static Map<String,String> READER_OPTIONS = new HashMap<>();
	static {
		READER_OPTIONS.put("header",     "true");
		READER_OPTIONS.put("delimiter",  ";");
	}
	
	/**
	 * SCRIPT file path
	 */
	private final static String SCRIPT_FILE_PATH = "D:/TMP/csv-files/books.script";

	/**
	 * Job constructor
	 */
	public BookJob() {
		
		super(	JOB_NAME, 
				JOB_MASTER_CONFIG, 
				INPUT_FILE_PATH );
		setReaderOptions(READER_OPTIONS);
	}

	public void run() throws Exception {
		
		String script = FileLoader.loadFile(SCRIPT_FILE_PATH);
		log("Script : \n" + script ) ;

		// SparkContext sparkContext = getSparkContext();

		// Declare job accumulators 
		LongAccumulator countAccumulator = getSparkContext().longAccumulator();
		
		// Launch 'foreach' ACTION 
		ForeachFunction<Row> foreachFunction = new BookForeachFunction(script, countAccumulator);
		foreach( foreachFunction );
		
		log("ACCUMULATOR = " + countAccumulator );
	}
}
