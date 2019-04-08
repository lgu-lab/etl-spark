package com.acme.person;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.demo.framework.script.AbstractForeachFunction;

public class PersonForeachFunction extends AbstractForeachFunction {

	private static final long serialVersionUID = 1L;

//	public PersonForeachFunction() {
//		super();
//	}

	public PersonForeachFunction(String script) throws Exception {
		super(script);
	}
	
	@Override
	public void preProcessing(Row row, Map<String,Object> map) throws Exception {
		
		map.put("id", Integer.parseInt( row.<String>getAs(0).trim() ) );
		map.put("firstName", row.<String>getAs(1).trim() );
		map.put("lastName", row.<String>getAs(2).trim() );
		log("In preProcessing : map = " + map);
	}

	@Override
	public void postProcessing(Row row, Map<String,Object> map) throws Exception {
		
		log("In postProcessing : map = " + map);
	}
}
