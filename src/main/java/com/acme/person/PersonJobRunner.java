package com.acme.person;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class PersonJobRunner {
	
	public static void main(String[] args) throws Exception {
		
		// Job initialization 
		PersonJob job = new PersonJob();
		
//		job.setReaderOptions(readerOptions);
//		job.foreach( new PersonForeachFunction(script) );
		
		job.run();
	}
}
