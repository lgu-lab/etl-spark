package com.acme.gldsold;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.sparkyflow.AbstractForeachFunction;
import org.sparkyflow.Accumulators;

public class GldsoldForeachFunction extends AbstractForeachFunction {

	private static final long serialVersionUID = 1L;
	
	public GldsoldForeachFunction(Accumulators accumulators, String script) throws Exception {
		super(accumulators,script);
		log("GldsoldForeachFunction CONSTRUCTOR");
	}
	
	@Override
	public void preProcessing(Row row, Map<String,Object> map) throws Exception {
		
//		map.put("id", Integer.parseInt( row.<String>getAs(0).trim() ) );
//		map.put("title", row.<String>getAs(1).trim() );
//		map.put("price", Double.parseDouble( row.<String>getAs(2).trim() ) );
//		log("In preProcessing : map = " + map);
	}

	@Override
	public void process(Row row, Map<String,Object> map) {
		// SPECIFIC PROCESSING FOR THIS JOB 
//		double price = (double) map.get("price");
//		map.put("price", price + 200);
	}
	
	@Override
	public void postProcessing(Row row, Map<String,Object> map) throws Exception {
//		log("In postProcessing : map = " + map);
	}
}
