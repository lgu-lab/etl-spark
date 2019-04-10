package com.acme.mercure;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.demo.framework.AbstractForeachFunction;

public class MercureForeachFunction extends AbstractForeachFunction {

	private static final long serialVersionUID = 1L;
	
	private final LongAccumulator countAccumulator;

	public MercureForeachFunction(String script, LongAccumulator countAccumulator) throws Exception {
		super(script);
		this.countAccumulator = countAccumulator;
		log("GldsoldForeachFunction CONSTRUCTOR");
	}
	
	@Override
	public void preProcessing(Row row, Map<String,Object> map) throws Exception {
		// PREPARE SCRIPT CONTEXT
		int i = 0 ;
		
		// INPUT VALUES (keep columns order)
		map.put("NUEDI", row.<String>getAs(i++).trim() );
		map.put("DAFCO", row.<String>getAs(i++).trim() );
		// Number values 
		map.put("FOROF", Double.parseDouble( row.<String>getAs(i++).trim() ) );
		map.put("FORRA", Double.parseDouble( row.<String>getAs(i++).trim() ) );
		map.put("FORRT", Double.parseDouble( row.<String>getAs(i++).trim() ) );
		map.put("FORSD", Double.parseDouble( row.<String>getAs(i++).trim() ) );
		map.put("FORFR", Double.parseDouble( row.<String>getAs(i++).trim() ) );
		map.put("CAFSD", Double.parseDouble( row.<String>getAs(i++).trim() ) );
		
		// OUTPUT VALUES (supposed to be computed in script)
		map.put("CAFDO", 0.0 );
		map.put("CAFRA", 0.0 );
		map.put("CAFDRE", 0.0 );
		map.put("RCCAFDP", 0.0 );
		map.put("CAFDF", 0.0 );
		
	}

// NO SPECIFIC PROCESSING FOR THIS JOB (just executue the job)
//	@Override
//	public void process(Row row, Map<String,Object> map) {
//	}
	
	@Override
	public void postProcessing(Row row, Map<String,Object> map) throws Exception {
		countAccumulator.add(1);
		log("In postProcessing : map = " + map);
		log(" CAFDO = " + map.get("CAFDO") + " / FOROF = " + map.get("FOROF") );
		log(" CAFRA = " + map.get("CAFRA") + " / FORRA = " + map.get("FORRA") );
		log(" CAFDRE = " + map.get("CAFDRE") + " / FORRT = " + map.get("FORRT") );
		log(" RCCAFDP = " + map.get("RCCAFDP")  );
		log(" CAFDF = " + map.get("CAFDF")  );
	}
}
