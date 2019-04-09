package com.acme.person;

import java.util.HashMap;
import java.util.Map;

import org.demo.framework.AbstractJob;
import org.demo.framework.FileLoader;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class PersonJob extends AbstractJob {
	
	/**
	 * Job name  (the 'application name' for the job)
	 */
	private final static String JOB_NAME = "PersonJob";

	/**
	 * Job master (the 'master' configuration for the job e.g. 'local[4]' )
	 */
	private final static String JOB_MASTER_CONFIG = "local[4]";

	/**
	 * DATA file path
	 */
	private final static String INPUT_FILE_PATH = "D:/TMP/csv-files/person.csv";
	// Other example : "hdfs://localhost:9002/data/data.csv"
	
	/**
	 * SCRIPT file path
	 */
	private final static String SCRIPT_FILE_PATH = "D:/TMP/csv-files/person.script";

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
	public PersonJob() {
		
		super(	JOB_NAME, 
				JOB_MASTER_CONFIG, 
				INPUT_FILE_PATH );
		setReaderOptions(READER_OPTIONS);
	}

	public void run() throws Exception {
		
		String script = FileLoader.loadFile(SCRIPT_FILE_PATH);
		log("Script : \n" + script ) ;
		
		// Job initialization 
//		setReaderOptions(readerOptions);

		//Dataset<Person> ds = job.getDataset();

		// Job actions ...
		
//		long count = job.count();
//		System.out.println("count = " + count);
//
//		Person person = job.first();
//		System.out.println("First is " + person);

		foreach( new PersonForeachFunction(script) );
		
//		Dataset<String> dsJSON = job.toJSON();
//		System.out.println("First is " + dsJSON.first());
//	

	}
}
