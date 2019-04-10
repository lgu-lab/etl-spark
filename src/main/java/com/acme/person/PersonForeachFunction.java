package com.acme.person;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.demo.framework.AbstractForeachFunction;
import org.demo.framework.Accumulators;

public class PersonForeachFunction extends AbstractForeachFunction {

	private static final long serialVersionUID = 1L;

	public PersonForeachFunction(Accumulators accumulators, String script) throws Exception {
		super(accumulators,script);
	}
	
	@Override
	public void preProcessing(Row row, Map<String,Object> map) throws Exception {
		
		map.put("id", getInteger(row, 0) );
		map.put("firstName", getString(row, 1) );
		map.put("lastName", getString(row, 2) );
		log("preProcessing : map = " + map);
	}

	@Override
	public void postProcessing(Row row, Map<String,Object> map) throws Exception {
		
		// TODO : Javascript double values management before post processing 
		Number n = (Number) map.get("id");
		map.put("id", n.intValue() );

		log("postProcessing : map = " + map);
		PersonDAO dao = new PersonDAO();
		log("postProcessing : saving in database...");
		dao.save(map);
	}
}
