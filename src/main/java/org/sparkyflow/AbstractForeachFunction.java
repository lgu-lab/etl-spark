package org.sparkyflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.sparkyflow.log.ErrorLogger;
import org.sparkyflow.log.StdOutErrorLogger;

public abstract class AbstractForeachFunction extends AbstractAction implements ForeachFunction<Row> {

	private static final long serialVersionUID = 1L;
	
	private final LongAccumulator rowCountAccumulator;
	private final LongAccumulator errCountAccumulator;
	private final ErrorLogger errorLogger;

	/**
	 * Constructor
	 * @param accumulators
	 */
	public AbstractForeachFunction(Accumulators accumulators ) {
		this(accumulators, null);
	}

	/**
	 * Constructor
	 * @param accumulators
	 * @param script
	 */
	public AbstractForeachFunction(Accumulators accumulators, String script) {
		super(script);
		this.rowCountAccumulator   = accumulators.get(Accumulators.ROW_COUNT);
		this.errCountAccumulator   = accumulators.get(Accumulators.ERR_COUNT);
		this.errorLogger = new StdOutErrorLogger();
	}
	
	/**
	 * Method for pre-processing (before 'call')
	 * @param row
	 * @param map
	 * @throws Exception
	 */
	public abstract void preProcessing(Row row, Map<String,Object>map) throws Exception ;
	
	/**
	 * Method for post-processing (after 'call')
	 * @param row
	 * @param map
	 * @throws Exception
	 */
	public abstract void postProcessing(Row row, Map<String,Object>map) throws Exception ;
	
	@Override
	public void call(Row row) throws Exception {
		
		// Increment 'row count' accumulator for the current worker/executor
		if ( rowCountAccumulator != null ) {
			rowCountAccumulator.add(1);
			logAccumulator("Foreach function", rowCountAccumulator);
		}
		
		// Try to process the row by calling the function
		try {
			internalCall(row);
		} catch (Exception e) {
			
			// Increment "error count" accumulator for the current worker/executor
			if ( errCountAccumulator != null ) {
				errCountAccumulator.add(1);
				logAccumulator("Foreach function", errCountAccumulator);
			}
			// Log error in error file
			errorLogger.log(e);
		}
	}
	
	private void internalCall(Row row) throws Exception {
		
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
	
	/**
	 * Returns the value of column i as STRING
	 * @param row
	 * @param i
	 * @return
	 */
	protected String getString(Row row, int i) {
		return row.<String>getAs(i).trim() ;
	}

	/**
	 * Returns the value of column i as DOUBLE
	 * @param row
	 * @param i
	 * @return
	 */
	protected Double getDouble(Row row, int i) {
		return Double.parseDouble( row.<String>getAs(i).trim() ) ;
	}

	/**
	 * Returns the value of column i as INTEGER
	 * @param row
	 * @param i
	 * @return
	 */
	protected Integer getInteger(Row row, int i) {
		return Integer.parseInt( row.<String>getAs(i).trim() );
	}

	/**
	 * Returns the value of column i as BOOLEAN
	 * @param row
	 * @param i
	 * @return
	 */
	protected Boolean getBoolean(Row row, int i) {
		String s = row.<String>getAs(i).trim() ;
		return s.equalsIgnoreCase("true");
	}
	
	protected void checkType(Map<String,Object> map, String key, Class<?> clazz) {
		Object o = map.get(key);
		if ( ! clazz.isInstance(o) ) {
			String msg = "Invalid type for '" + key + " : " + o.getClass().getName() + " (" +clazz.getName() + " expected)" ;
			log(msg);
			throw new RuntimeException(msg);
		}
	}
	
}
