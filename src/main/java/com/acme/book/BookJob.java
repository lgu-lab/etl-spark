package com.acme.book;

import java.util.HashMap;
import java.util.Map;

import org.demo.framework.job.AbstractJob;

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
	 * Job constructor
	 */
	public BookJob() {
		
		super(	JOB_NAME, 
				JOB_MASTER_CONFIG, 
				INPUT_FILE_PATH );
		setReaderOptions(READER_OPTIONS);
	}

	public void run() throws Exception {
		
		String script = "" 
				+ "print('In Javascript');"
				+ "// Compute  \n"
				+ "price = price * 1.2 ; \n"
				;

		// Job initialization 
//		setReaderOptions(readerOptions);

		//Dataset<Person> ds = job.getDataset();

		// Job actions ...
		
//		long count = job.count();
//		System.out.println("count = " + count);
//
//		Person person = job.first();
//		System.out.println("First is " + person);

		foreach( new BookForeachFunction(script) );
		
//		Dataset<String> dsJSON = job.toJSON();
//		System.out.println("First is " + dsJSON.first());
//	

	}
}
