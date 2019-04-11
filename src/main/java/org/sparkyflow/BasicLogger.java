package org.sparkyflow;

import org.apache.spark.util.LongAccumulator;

public class BasicLogger {

	public static void log(String msg) {
		if ( LogConfig.LOG ) {
			System.out.println("[LOG] " + msg );
			System.out.flush();
		}		
	}
	
	public static void logAccumulator(LongAccumulator accumulator )  {
		BasicLogger.log(accumulatorMsg(accumulator) );
	}
	
	public static void logAccumulator(String s, LongAccumulator accumulator )  {
		BasicLogger.log(s + " - " + accumulatorMsg(accumulator) );
	}
	
	private static String accumulatorMsg(LongAccumulator accumulator) {
		return "ACCUMULATOR = " + accumulator.toString()
		+ " / "
		+ "count = " + accumulator.count() 
		+ " / "
		+ "value = " + accumulator.value() 
		+ " / "
		+ ( accumulator.isAtDriverSide() ? "driver side" : "worker side" )
		;
	}
}
