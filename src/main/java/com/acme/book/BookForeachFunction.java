package com.acme.book;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.demo.framework.AbstractForeachFunction;

public class BookForeachFunction extends AbstractForeachFunction {

	private static final long serialVersionUID = 1L;
	
	private final LongAccumulator countAccumulator;

	public BookForeachFunction(String script, LongAccumulator countAccumulator) throws Exception {
		super(script);
		this.countAccumulator = countAccumulator;
		log("BookForeachFunction CONSTRUCTOR");
	}
	
	@Override
	public void preProcessing(Row row, Map<String,Object> map) throws Exception {
		
		map.put("id", Integer.parseInt( row.<String>getAs(0).trim() ) );
		map.put("title", row.<String>getAs(1).trim() );
		map.put("price", Double.parseDouble( row.<String>getAs(2).trim() ) );
		log("In preProcessing : map = " + map);
	}

//	@Override
//	public void process(Row row, Map<String,Object> map) {
//		// SPECIFIC PROCESSING FOR THIS JOB 
//		double price = (double) map.get("price");
//		map.put("price", price + 200);
//	}
	
	@Override
	public void postProcessing(Row row, Map<String,Object> map) throws Exception {
		countAccumulator.add(1);
		log("In postProcessing : map = " + map);
	}
}
