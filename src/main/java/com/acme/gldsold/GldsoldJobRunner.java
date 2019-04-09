package com.acme.gldsold;

/**
 * Job definition to process a file 
 * 
 * @author laguerin
 *
 */
public class GldsoldJobRunner {
	
	public static void main(String[] args) throws Exception {
		
		// Job initialization 
		GldsoldJob job = new GldsoldJob();
		
		job.run();
	}
}
