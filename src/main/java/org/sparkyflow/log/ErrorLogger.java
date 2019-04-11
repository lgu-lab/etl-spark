package org.sparkyflow.log;

public interface ErrorLogger {

	public void log(Exception e) ;
	
	public void log(String msg, Exception e) ;
	
	public void log(String msg) ;
	
}
