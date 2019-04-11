package org.sparkyflow.log;

public class StdOutErrorLogger implements ErrorLogger {

	public static boolean LOG = true ;
	
	public static void print(String msg) {
		if ( LOG ) {
			System.out.println("[ERROR] " + msg );
			System.out.flush();
		}		
	}

	@Override
	public void log(Exception e) {
		// TODO Auto-generated method stub
		print("Exception : " + e.getClass().getName() + " : " + e.getMessage() );
	}

	@Override
	public void log(String msg, Exception e) {
		print(msg + " Exception : " + e.getClass().getName() + " : " + e.getMessage() );
	}

	@Override
	public void log(String msg) {
		print(msg);
	}

}
