package org.demo.framework;

public class BasicLogger {

	public static void log(String msg) {
		if ( LogConfig.LOG ) {
			System.out.println("[LOG] " + msg );
			System.out.flush();
		}		
	}
}
