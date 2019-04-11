package org.sparkyflow;

import java.util.Map;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.sparkyflow.log.BasicLogger;

public abstract class AbstractJob {
	
	private final String   jobName ;
	private final String   jobMaster ;
	private final String   filePath ;
	private final Map<String,String> fileReaderOptions ;
	private final String scriptFilePath ;
	
	private final SparkSession sparkSession ;
	private final Accumulators accumulators ;

	private Dataset<Row> dataset = null ;
	
	/**
	 * Constructor 
	 * @param jobName the 'application name' for the job (used as 'appName(jobName)' ) 
	 * @param jobMaster the 'master' configuration for the job e.g. 'local[4]' (used by 'master("local[4]")' ) 
	 * @param filePath
	 * @param fileReaderOptions
	 * @param scriptFilePath
	 */
	public AbstractJob(String jobName, String jobMaster,  String filePath, Map<String,String> fileReaderOptions, String scriptFilePath ) {
		super();
		this.jobName   = jobName;
		this.jobMaster = jobMaster;
		this.filePath  = filePath ;
		this.fileReaderOptions = fileReaderOptions ;
		this.scriptFilePath = scriptFilePath ;

		log("Creating SparkSession...");
    	this.sparkSession = SparkSession.builder()
    			.appName(jobName)
    			.master(jobMaster) 
    			.config("spark.ui.enabled", false)
    			.getOrCreate();
		log("SparkSession ready.");
		
		this.accumulators = new Accumulators();
		// Create and register "row-count" accumulator
		LongAccumulator rowCountAccumulator = getSparkContext().longAccumulator(Accumulators.ROW_COUNT);
		accumulators.register(Accumulators.ROW_COUNT, rowCountAccumulator);
		// Create and register "row-count" accumulator
		LongAccumulator errCountAccumulator = getSparkContext().longAccumulator(Accumulators.ERR_COUNT);
		accumulators.register(Accumulators.ERR_COUNT, errCountAccumulator);
		
	}

	protected String getName() {
		return this.jobName;
	}

	protected String getMaster() {
		return this.jobMaster;
	}

	protected SparkSession getSparkSession() {
		return this.sparkSession;
	}
		
	protected SparkContext getSparkContext() {
		return this.sparkSession.sparkContext();
	}
	
	protected String loadScript() {
		if ( scriptFilePath != null && ! scriptFilePath.trim().isEmpty() ) {
			return FileLoader.loadFile(scriptFilePath);
		}
		return null ;
	}
	
	protected Accumulators getAccumulators() {
		return this.accumulators;
	}
	protected LongAccumulator getAccumulator(String name) {
		return this.accumulators.get(name);
	}
	
	protected void log(String msg) {
		BasicLogger.log(msg);
	}
	
	protected void logAccumulator(String s, LongAccumulator accumulator )  {
		BasicLogger.logAccumulator(s, accumulator);
	}

//	public void setReaderOptions(Map<String,String> options) {
//		this.dataFrameReaderOptions = options ;
//	}
	
	private Dataset<Row> createDataset() {
		
		log("Creating DataFrameReader...");
    	DataFrameReader dataFrameReader = sparkSession.read()
	    	.option("header",    "true")
	    	.option("delimiter", ";")
	    	.format("csv");
    	
		log("Applying DataFrameReader options...");
//    	dataFrameReader.options(this.dataFrameReaderOptions);
		if ( fileReaderOptions != null ) {
	    	dataFrameReader.options(fileReaderOptions);
		}
    	
		log("DataFrameReader ready.");    	
		
		log("Loading file to initial Dataset (Dataset<Row>)...");
    	Dataset<Row> initialDataset = dataFrameReader.load(filePath);    	
		log("Dataset<Row> ready."); // Not realy loaded

//		// TRANSFORMATION : map to JAVA BEAN 
//		log("Mapping initial Dataset to DataSet of beans (Dataset<Row>)...");
//		Dataset<Row> dataset = initialDataset.map(mappingFunction, Encoders.bean(Row.class) );
//		log("Dataset<Row> ready."); 
		
		dataset = initialDataset ;
		
		StructType schema = dataset.schema();
		log("SCHEMA  (length = " + schema.length() + ") : ");
		for ( String s : schema.fieldNames() ) {
			log(" . " + s );
		}
		log("SCHEMA  StructFields : ");
		for ( StructField structField : schema.fields() ) {
			log(" . " + structField.name() + " : dataType = " + structField.dataType() + " / nullable = " + structField.nullable());
		}
		return dataset ;
	}
	
	protected Dataset<Row> getDataset() {
		log("getDataset()" ); 	
		if ( dataset == null ) {
			dataset = createDataset();
		}
		return dataset ;
	}
	
	/**
	 * SPARK ACTION : 'foreach'
	 */
	protected void foreach(ForeachFunction<Row> foreachFunction) {
		
		log("Sarting 'foreach' action... " ); 	
		Dataset<Row> dataset = getDataset();
    	dataset.foreach(foreachFunction);
    	log("End of 'foreach' action. " ); 	
	}

	/**
	 * SPARK ACTION : 'count'
	 * @return
	 */
	protected long count() {
		
		log("Sarting 'count' action... " ); 	
		Dataset<Row> dataset = getDataset();
    	long count = dataset.count();
    	log("End of 'count' action (count=" + count + "). " );
    	return count ;
	}

	/**
	 * SPARK ACTION : 'toJSON'
	 * @return
	 */
	protected Dataset<String> toJSON() {
		
		log("Sarting 'toJSON' action... " ); 	
		Dataset<Row> dataset = getDataset();
		Dataset<String> result = dataset.toJSON();
    	log("End of 'toJSON' action." );
    	return result ;
	}

	/**
	 * SPARK ACTION : 'first'
	 * @return
	 */
	protected Row first() {
		
		log("Sarting 'first' action... " ); 	
		Dataset<Row> dataset = getDataset();
		Row item = dataset.first();
    	log("End of 'first' action. " );
    	return item ;
	}

	protected void write() {
		
		log("Sarting 'write' action... " ); 	
		Dataset<Row> dataset = getDataset();
    	dataset.write();
    	log("End of 'write' action. " );
	}

	protected void save(String filePath) {
		
		log("Sarting 'write' action... " ); 	
		Dataset<Row> dataset = getDataset();
    	//dataset.write();
    	
		if ( ! dataset.isEmpty() ) {
			log("Dataset is not empty. Trying to save..." ); 	
	    	dataset.write()
	    	.format("com.databricks.spark.csv")
//	    	.option("inferSchema", "false")
	    	.option("header", "true")
	    	.option("charset", "UTF-8")
//	    	.option("escape", escape)
	    	.option("delimiter", "|")
	    	.option("quote", "\'")
	    	.mode(SaveMode.Overwrite)
	    	.save(filePath);
		}
		else {
			log("dataset is empty!" ); 	
		}
    	
    	log("End of 'write' action. " );
	}
	
}
