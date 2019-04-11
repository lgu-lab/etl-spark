package org.sparkyflow;

import org.apache.spark.util.LongAccumulator;
import org.sparkyflow.javascript.AbstractScriptExecutor;
import org.sparkyflow.log.BasicLogger;

public class AbstractAction extends AbstractScriptExecutor {

	private static final long serialVersionUID = 1L;

	public AbstractAction(String script) {
		super(script);
	}

	protected void log(String msg) {
		BasicLogger.log(msg);
	}
	
	protected void logAccumulator(String s, LongAccumulator accumulator) {
		BasicLogger.logAccumulator(s, accumulator);
	}
	

}
