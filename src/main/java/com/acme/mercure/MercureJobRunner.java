package com.acme.mercure;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class MercureJobRunner {
	
	public static void main(String[] args) throws Exception {
		
		// Job initialization 
		MercureJob job = new MercureJob();
		
		job.run();
	}
}
