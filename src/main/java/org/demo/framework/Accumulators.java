package org.demo.framework;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.util.LongAccumulator;

public class Accumulators {

	public final static String ROW_COUNT = "row-count" ;

	public final static String ERR_COUNT = "err-count" ;
	
	private final Map<String,LongAccumulator> accumulators = new HashMap<>();
	
	public void register(String name, LongAccumulator accumulator) {
		accumulators.put(name, accumulator);
	}
	
	public LongAccumulator get(String name) {
		return accumulators.get(name);
	}
	
}
