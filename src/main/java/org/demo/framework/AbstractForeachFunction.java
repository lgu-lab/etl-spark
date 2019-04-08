package org.demo.framework;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;

public abstract class AbstractForeachFunction extends AbstractScriptExecutor implements ForeachFunction<Row> {

	private static final long serialVersionUID = 1L;

	public AbstractForeachFunction() {
		super();
	}

	public AbstractForeachFunction(String script) throws Exception {
		super(script);
	}
	
	public abstract void preProcessing(Row row, Map<String,Object>map) throws Exception ;
	
	public abstract void postProcessing(Row row, Map<String,Object>map) throws Exception ;
	
	@Override
	public void call(Row row) throws Exception {
		
		// Get Spark partition ID 
		int partitionId = TaskContext.getPartitionId(); // get from ThreadLocal
		long taskId = TaskContext.get().taskAttemptId(); 
		log("call(Row) : Task = " + taskId + " / Partition = " + partitionId );

		// Put data in a map (the row always remains unchanged) 
		Map<String,Object> map = new HashMap<>();

		log("call(Row) : preProcessing ... ");
		preProcessing(row, map);
		
		// TRANSFORM THE CURRENT ROW USING A SCRIPT 
//		//executeScript(genericRow.getMap());
//		log("call(Row) : execute script ... ");
//		executeScript(map);
//		log("call(Row) : script executed." );
		process(row, map);
		
		log("call(Row) : postProcessing ... ");
		postProcessing(row, map);
		log("call(Row) : end. ");		
	}
	
	/**
	 * Standard processing : script execution <br>
	 * For specific processing just override this method
	 * @param row
	 * @param map
	 * @throws Exception
	 */
	public void process(Row row, Map<String,Object> map) throws Exception {
		log("call(Row) : process -> executeScript ... ");
		executeScript(map);
		log("call(Row) : script executed." );
	}
}
