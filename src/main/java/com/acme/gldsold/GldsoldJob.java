package com.acme.gldsold;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.sparkyflow.AbstractJob;
import org.sparkyflow.Accumulators;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class GldsoldJob extends AbstractJob {
	
	/**
	 * Job name  (the 'application name' for the job)
	 */
	private final static String JOB_NAME = "GldsoldJob";

	/**
	 * Job master (the 'master' configuration for the job e.g. 'local[4]' )
	 */
	private final static String JOB_MASTER_CONFIG = "local[4]";

	/**
	 * Input file path
	 */
	private final static String INPUT_FILE_PATH = "D:/TMP/csv-files/GLDSOLD.CSV";
	
	/**
	 * Input file reader options
	 */
	private final static Map<String,String> READER_OPTIONS = new HashMap<>();
	static {
		READER_OPTIONS.put("header",     "true");
		READER_OPTIONS.put("delimiter",  "|");
	}
	
	/**
	 * Job constructor
	 */
	public GldsoldJob() {
		
		super(	JOB_NAME, 
				JOB_MASTER_CONFIG, 
				INPUT_FILE_PATH,
				READER_OPTIONS,
				null);
//		setReaderOptions(READER_OPTIONS);
	}

	public void run() throws Exception {
		
//		String script = "" 
//				+ "print('In Javascript');"
//				+ "// Compute  \n"
//				+ "price = price * 1.2 ; \n"
//				;
		
		String script = "" ; // NO SCRIPT
		
		// Launch 'foreach' ACTION 
		ForeachFunction<Row> foreachFunction = new GldsoldForeachFunction(getAccumulators(), script);
		foreach( foreachFunction );
		
		// RESULT 
		LongAccumulator errCount = getAccumulator(Accumulators.ERR_COUNT);
		logAccumulator("End of job", getAccumulator(Accumulators.ROW_COUNT) );
		logAccumulator("End of job", errCount );
		if ( errCount.value() > 0 ) {
			System.out.println("\n   /!\\  " + errCount.value() + " ERROR(S)  /!\\  \n");
		}

	}
}
