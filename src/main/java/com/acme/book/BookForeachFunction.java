package com.acme.book;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.demo.framework.AbstractForeachFunction;
import org.demo.framework.Accumulators;

public class BookForeachFunction extends AbstractForeachFunction {

	private static final long serialVersionUID = 1L;
	
	public BookForeachFunction(Accumulators accumulators, String script) throws Exception {
		super(accumulators, script);
		log("BookForeachFunction CONSTRUCTOR");
	}
	
	@Override
	public void preProcessing(Row row, Map<String,Object> map) throws Exception {
		
		map.put("id",    getInteger(row, 0) );
		map.put("title", getString(row, 1) );
		map.put("price", getDouble(row, 2) );
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
		log("In postProcessing : map = " + map);
	}
}
