package com.acme.book;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class BookJobRunner {
	
	public static void main(String[] args) throws Exception {
		
		// Job initialization 
		BookJob job = new BookJob();
		
//		job.setReaderOptions(readerOptions);
//		job.foreach( new PersonForeachFunction(script) );
		
		job.run();
	}
}
