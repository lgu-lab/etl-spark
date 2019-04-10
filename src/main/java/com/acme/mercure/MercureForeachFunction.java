package com.acme.mercure;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.demo.framework.AbstractForeachFunction;
import org.demo.framework.Accumulators;

public class MercureForeachFunction extends AbstractForeachFunction {

	private static final long serialVersionUID = 1L;
	
	// private final LongAccumulator countAccumulator;

	//public MercureForeachFunction(String script, LongAccumulator countAccumulator) throws Exception {
//	public MercureForeachFunction(SparkSession sparkSession, String script) throws Exception {
	public MercureForeachFunction(Accumulators accumulators, String script) throws Exception {
		super(accumulators, script);
//		this.countAccumulator = countAccumulator;
		log("ForeachFunction CONSTRUCTOR");
	}
	
	@Override
	public void preProcessing(Row row, Map<String,Object> map) throws Exception {
		// PREPARE SCRIPT CONTEXT
		int i = 0 ;
		
		// INPUT VALUES (keep columns order)
		map.put("NUEDI", getString(row,i++) );
		map.put("DAFCO", getString(row,i++) );
		// Number values 
		map.put("FOROF", getDouble(row,i++) );
		map.put("FORRA", getDouble(row,i++) );
		map.put("FORRT", getDouble(row,i++) );
		map.put("FORSD", getDouble(row,i++) );
		map.put("FORFR", getDouble(row,i++) );
		map.put("CAFSD", getDouble(row,i++) );
		
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
//		countAccumulator.add(1);
		log("In postProcessing : map = " + map);
		log(" CAFDO = " + map.get("CAFDO") + " / FOROF = " + map.get("FOROF") );
		log(" CAFRA = " + map.get("CAFRA") + " / FORRA = " + map.get("FORRA") );
		log(" CAFDRE = " + map.get("CAFDRE") + " / FORRT = " + map.get("FORRT") );
		log(" RCCAFDP = " + map.get("RCCAFDP")  );
		log(" CAFDF = " + map.get("CAFDF")  );
	}
}
