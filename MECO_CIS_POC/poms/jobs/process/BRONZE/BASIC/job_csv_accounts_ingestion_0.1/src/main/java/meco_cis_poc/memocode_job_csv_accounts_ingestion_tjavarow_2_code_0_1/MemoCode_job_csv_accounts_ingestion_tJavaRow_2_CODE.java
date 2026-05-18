
package meco_cis_poc.memocode_job_csv_accounts_ingestion_tjavarow_2_code_0_1;

import routines.DataOperation;
import routines.TalendDataGenerator;
import routines.DataQuality;
import routines.Relational;
import routines.Mathematical;
import routines.DataQualityDependencies;
import routines.SQLike;
import routines.Numeric;
import routines.TalendStringUtil;
import routines.TalendString;
import routines.MDM;
import routines.StringHandling;
import routines.DQTechnical;
import routines.TalendDate;
import routines.DataMasking;
import routines.DqStringHandling;
import routines.system.*;
import routines.system.api.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.math.BigDecimal;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.Comparator;
 




//the import part of tJavaRow_2
import routines.DataOperation;
import routines.TalendDataGenerator;
import routines.DataQuality;
import routines.Relational;
import routines.Mathematical;
import routines.DataQualityDependencies;
import routines.SQLike;
import routines.Numeric;
import routines.TalendStringUtil;
import routines.TalendString;
import routines.MDM;
import routines.StringHandling;
import routines.DQTechnical;
import routines.TalendDate;
import routines.DataMasking;
import routines.DqStringHandling;


// __studio_memo_code__ IMPORT begin__

//import java.util.List;
// __studio_memo_code__ IMPORT end__


@SuppressWarnings("unused")

/**
 * Job: MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE Purpose: <br>
 * Description:  <br>
 * @author Parras, Tom
 * @version 8.0.1.20260102_0846-patch
 * @status 
 */
public class MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE implements TalendJob {
	static {System.setProperty("TalendJob.log", "MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE.log");}

	

	
	private static org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager.getLogger(MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE.class);
	

static {
		 System.setProperty("talend.component.record.nullable.check", "true");
         String javaUtilLoggingConfigFile = System.getProperty("java.util.logging.config.file");
         if (javaUtilLoggingConfigFile == null) {
             setupDefaultJavaUtilLogging();
         }
}

/**
* This class replaces the default {@code System.err} stream used by Java Util Logging (JUL).
* You can use your own configuration through the
* {@code java.util.logging.config.file} system property, enabling you to specify an external
* logging configuration file for tailored logging setup.
*/
public static class StandardConsoleHandler extends java.util.logging.StreamHandler {
     public StandardConsoleHandler() {
         // Set System.out as default log output stream
         super(System.out, new java.util.logging.SimpleFormatter());
     }

     /**
      * Publish a {@code LogRecord}.
      * The logging request was made initially to a {@code Logger} object,
      * which initialized the {@code LogRecord} and forwarded it here.
      *
      * @param  record  description of the log event. A null record is
      *                 silently ignored and is not published
      */
      @Override
      public void publish(java.util.logging.LogRecord record) {
            super.publish(record);
            flush();
      }

      /**
      * Override {@code StreamHandler.close} to do a flush but not
      * to close the output stream.  That is, we do <b>not</b>
      * close {@code System.out}.
      */
      @Override
      public void close() {
            flush();
      }
}


protected static void setupDefaultJavaUtilLogging() {
      java.util.logging.LogManager logManager = java.util.logging.LogManager.getLogManager();

      // Get the root logger
      java.util.logging.Logger rootLogger = logManager.getLogger("");

      // Remove existing handlers to set standard console handler only
      java.util.logging.Handler[] handlers = rootLogger.getHandlers();
      for (java.util.logging.Handler handler : handlers) {
            rootLogger.removeHandler(handler);
      }

      rootLogger.addHandler(new StandardConsoleHandler());
      rootLogger.setLevel(java.util.logging.Level.INFO);
}

protected static boolean isCBPClientPresent() {
    boolean isCBPClientPresent = false;
    try {
         Class.forName("org.talend.metrics.CBPClient");
         isCBPClientPresent = true;
        } catch (java.lang.ClassNotFoundException e) {
    }
    return isCBPClientPresent;
}

protected static void logIgnoredError(String message, Throwable cause) {
       log.error(message, cause);

}


	public final Object obj = new Object();

	// for transmiting parameters purpose
	private Object valueObject = null;

	public Object getValueObject() {
		return this.valueObject;
	}

	public void setValueObject(Object valueObject) {
		this.valueObject = valueObject;
	}
	
	private final static String defaultCharset = java.nio.charset.Charset.defaultCharset().name();

	
	private final static String utf8Charset = "UTF-8";

	public static String taskExecutionId = null;

	public static String jobExecutionId = java.util.UUID.randomUUID().toString();;

	private final static boolean isCBPClientPresent = isCBPClientPresent();

    public static final java.util.List<Thread> threadList = java.util.Collections.synchronizedList(new java.util.ArrayList<>());	

	//contains type for every context property
	public class PropertiesWithType extends java.util.Properties {
		private static final long serialVersionUID = 1L;
		private java.util.Map<String,String> propertyTypes = new java.util.HashMap<>();
		
		public PropertiesWithType(java.util.Properties properties){
			super(properties);
		}
		public PropertiesWithType(){
			super();
		}
		
		public void setContextType(String key, String type) {
			propertyTypes.put(key,type);
		}
	
		public String getContextType(String key) {
			return propertyTypes.get(key);
		}
	}	
	// create and load default properties
	private java.util.Properties defaultProps = new java.util.Properties();
		

	// create application properties with default
	public class ContextProperties extends PropertiesWithType {

		private static final long serialVersionUID = 1L;

		public ContextProperties(java.util.Properties properties){
			super(properties);
		}
		public ContextProperties(){
			super();
		}

		public void synchronizeContext(){
			
			if(conn_hive_target_table_AdditionalJDBCParameters != null){
				
					this.setProperty("conn_hive_target_table_AdditionalJDBCParameters", conn_hive_target_table_AdditionalJDBCParameters.toString());
				
			}
			
			if(conn_hive_target_table_Database != null){
				
					this.setProperty("conn_hive_target_table_Database", conn_hive_target_table_Database.toString());
				
			}
			
			if(conn_hive_target_table_Password != null){
				
					this.setProperty("conn_hive_target_table_Password", conn_hive_target_table_Password.toString());
				
			}
			
			if(conn_hive_target_table_Server != null){
				
					this.setProperty("conn_hive_target_table_Server", conn_hive_target_table_Server.toString());
				
			}
			
			if(conn_hive_target_table_HiveMetastorePort != null){
				
					this.setProperty("conn_hive_target_table_HiveMetastorePort", conn_hive_target_table_HiveMetastorePort.toString());
				
			}
			
			if(conn_hive_target_table_Port != null){
				
					this.setProperty("conn_hive_target_table_Port", conn_hive_target_table_Port.toString());
				
			}
			
			if(conn_hive_target_table_Login != null){
				
					this.setProperty("conn_hive_target_table_Login", conn_hive_target_table_Login.toString());
				
			}
			
			if(conn_sftp_mbtc_username != null){
				
					this.setProperty("conn_sftp_mbtc_username", conn_sftp_mbtc_username.toString());
				
			}
			
			if(conn_sftp_mbtc_host != null){
				
					this.setProperty("conn_sftp_mbtc_host", conn_sftp_mbtc_host.toString());
				
			}
			
			if(conn_sftp_mbtc_port != null){
				
					this.setProperty("conn_sftp_mbtc_port", conn_sftp_mbtc_port.toString());
				
			}
			
			if(conn_sftp_mbtc_password != null){
				
					this.setProperty("conn_sftp_mbtc_password", conn_sftp_mbtc_password.toString());
				
			}
			
			if(conn_hadoop_cluster_User != null){
				
					this.setProperty("conn_hadoop_cluster_User", conn_hadoop_cluster_User.toString());
				
			}
			
			if(conn_hadoop_cluster_NameNodeUri != null){
				
					this.setProperty("conn_hadoop_cluster_NameNodeUri", conn_hadoop_cluster_NameNodeUri.toString());
				
			}
			
			if(target_count != null){
				
					this.setProperty("target_count", target_count.toString());
				
			}
			
			if(src_file_dir != null){
				
					this.setProperty("src_file_dir", src_file_dir.toString());
				
			}
			
			if(filename != null){
				
					this.setProperty("filename", filename.toString());
				
			}
			
			if(file_extension != null){
				
					this.setProperty("file_extension", file_extension.toString());
				
			}
			
			if(hdfs_raw_src_file_dir != null){
				
					this.setProperty("hdfs_raw_src_file_dir", hdfs_raw_src_file_dir.toString());
				
			}
			
			if(source_schema != null){
				
					this.setProperty("source_schema", source_schema.toString());
				
			}
			
			if(target_schema != null){
				
					this.setProperty("target_schema", target_schema.toString());
				
			}
			
			if(target_tablename != null){
				
					this.setProperty("target_tablename", target_tablename.toString());
				
			}
			
			if(target_table_location != null){
				
					this.setProperty("target_table_location", target_table_location.toString());
				
			}
			
			if(create_table_schema != null){
				
					this.setProperty("create_table_schema", create_table_schema.toString());
				
			}
			
			if(select_columns != null){
				
					this.setProperty("select_columns", select_columns.toString());
				
			}
			
			if(source_count != null){
				
					this.setProperty("source_count", source_count.toString());
				
			}
			
			if(job_server_file_dir != null){
				
					this.setProperty("job_server_file_dir", job_server_file_dir.toString());
				
			}
			
			if(audit_database != null){
				
					this.setProperty("audit_database", audit_database.toString());
				
			}
			
			if(manifest_table != null){
				
					this.setProperty("manifest_table", manifest_table.toString());
				
			}
			
			if(conn_oracle_source_table_Password != null){
				
					this.setProperty("conn_oracle_source_table_Password", conn_oracle_source_table_Password.toString());
				
			}
			
			if(conn_oracle_source_table_Schema != null){
				
					this.setProperty("conn_oracle_source_table_Schema", conn_oracle_source_table_Schema.toString());
				
			}
			
			if(conn_oracle_source_table_Server != null){
				
					this.setProperty("conn_oracle_source_table_Server", conn_oracle_source_table_Server.toString());
				
			}
			
			if(conn_oracle_source_table_Port != null){
				
					this.setProperty("conn_oracle_source_table_Port", conn_oracle_source_table_Port.toString());
				
			}
			
			if(conn_oracle_source_table_ServiceName != null){
				
					this.setProperty("conn_oracle_source_table_ServiceName", conn_oracle_source_table_ServiceName.toString());
				
			}
			
			if(conn_oracle_source_table_Login != null){
				
					this.setProperty("conn_oracle_source_table_Login", conn_oracle_source_table_Login.toString());
				
			}
			
			if(conn_oracle_source_table__AdditionalParams != null){
				
					this.setProperty("conn_oracle_source_table__AdditionalParams", conn_oracle_source_table__AdditionalParams.toString());
				
			}
			
			if(field_separator != null){
				
					this.setProperty("field_separator", field_separator.toString());
				
			}
			
			if(header != null){
				
					this.setProperty("header", header.toString());
				
			}
			
		}
		
		//if the stored or passed value is "<TALEND_NULL>" string, it mean null
		public String getStringValue(String key) {
			String origin_value = this.getProperty(key);
			if(NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY.equals(origin_value)) {
				return null;
			}
			return origin_value;
		}

public String conn_hive_target_table_AdditionalJDBCParameters;
public String getConn_hive_target_table_AdditionalJDBCParameters(){
	return this.conn_hive_target_table_AdditionalJDBCParameters;
}
public String conn_hive_target_table_Database;
public String getConn_hive_target_table_Database(){
	return this.conn_hive_target_table_Database;
}
public java.lang.String conn_hive_target_table_Password;
public java.lang.String getConn_hive_target_table_Password(){
	return this.conn_hive_target_table_Password;
}
public String conn_hive_target_table_Server;
public String getConn_hive_target_table_Server(){
	return this.conn_hive_target_table_Server;
}
public String conn_hive_target_table_HiveMetastorePort;
public String getConn_hive_target_table_HiveMetastorePort(){
	return this.conn_hive_target_table_HiveMetastorePort;
}
public String conn_hive_target_table_Port;
public String getConn_hive_target_table_Port(){
	return this.conn_hive_target_table_Port;
}
public String conn_hive_target_table_Login;
public String getConn_hive_target_table_Login(){
	return this.conn_hive_target_table_Login;
}
public String conn_sftp_mbtc_username;
public String getConn_sftp_mbtc_username(){
	return this.conn_sftp_mbtc_username;
}
public String conn_sftp_mbtc_host;
public String getConn_sftp_mbtc_host(){
	return this.conn_sftp_mbtc_host;
}
public Integer conn_sftp_mbtc_port;
public Integer getConn_sftp_mbtc_port(){
	return this.conn_sftp_mbtc_port;
}
public java.lang.String conn_sftp_mbtc_password;
public java.lang.String getConn_sftp_mbtc_password(){
	return this.conn_sftp_mbtc_password;
}
public String conn_hadoop_cluster_User;
public String getConn_hadoop_cluster_User(){
	return this.conn_hadoop_cluster_User;
}
public String conn_hadoop_cluster_NameNodeUri;
public String getConn_hadoop_cluster_NameNodeUri(){
	return this.conn_hadoop_cluster_NameNodeUri;
}
public Integer target_count;
public Integer getTarget_count(){
	return this.target_count;
}
public String src_file_dir;
public String getSrc_file_dir(){
	return this.src_file_dir;
}
public String filename;
public String getFilename(){
	return this.filename;
}
public String file_extension;
public String getFile_extension(){
	return this.file_extension;
}
public String hdfs_raw_src_file_dir;
public String getHdfs_raw_src_file_dir(){
	return this.hdfs_raw_src_file_dir;
}
public String source_schema;
public String getSource_schema(){
	return this.source_schema;
}
public String target_schema;
public String getTarget_schema(){
	return this.target_schema;
}
public String target_tablename;
public String getTarget_tablename(){
	return this.target_tablename;
}
public String target_table_location;
public String getTarget_table_location(){
	return this.target_table_location;
}
public String create_table_schema;
public String getCreate_table_schema(){
	return this.create_table_schema;
}
public String select_columns;
public String getSelect_columns(){
	return this.select_columns;
}
public Integer source_count;
public Integer getSource_count(){
	return this.source_count;
}
public String job_server_file_dir;
public String getJob_server_file_dir(){
	return this.job_server_file_dir;
}
public String audit_database;
public String getAudit_database(){
	return this.audit_database;
}
public String manifest_table;
public String getManifest_table(){
	return this.manifest_table;
}
public java.lang.String conn_oracle_source_table_Password;
public java.lang.String getConn_oracle_source_table_Password(){
	return this.conn_oracle_source_table_Password;
}
public String conn_oracle_source_table_Schema;
public String getConn_oracle_source_table_Schema(){
	return this.conn_oracle_source_table_Schema;
}
public String conn_oracle_source_table_Server;
public String getConn_oracle_source_table_Server(){
	return this.conn_oracle_source_table_Server;
}
public String conn_oracle_source_table_Port;
public String getConn_oracle_source_table_Port(){
	return this.conn_oracle_source_table_Port;
}
public String conn_oracle_source_table_ServiceName;
public String getConn_oracle_source_table_ServiceName(){
	return this.conn_oracle_source_table_ServiceName;
}
public String conn_oracle_source_table_Login;
public String getConn_oracle_source_table_Login(){
	return this.conn_oracle_source_table_Login;
}
public String conn_oracle_source_table__AdditionalParams;
public String getConn_oracle_source_table__AdditionalParams(){
	return this.conn_oracle_source_table__AdditionalParams;
}
public String field_separator;
public String getField_separator(){
	return this.field_separator;
}
public Integer header;
public Integer getHeader(){
	return this.header;
}
	}
			
	protected ContextProperties context = new ContextProperties(); // will be instanciated by MS.
	public ContextProperties getContext() {
		return this.context;
	}

	protected java.util.Map<String, String> defaultProperties = new java.util.HashMap<String, String>();
	protected java.util.Map<String, String> additionalProperties = new java.util.HashMap<String, String>();

	public java.util.Map<String, String> getDefaultProperties() {
	    return this.defaultProperties;
	}

	public java.util.Map<String, String> getAdditionalProperties() {
        return this.additionalProperties;
    }

	private final String jobVersion = "0.1";
	private final String jobName = "MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE";
	private final String projectName = "MECO_CIS_POC";
	public Integer errorCode = null;
	private String currentComponent = "";
	public static boolean isStandaloneMS = Boolean.valueOf("false");
	
	private void s(final String component) {
		try {
			org.talend.metrics.DataReadTracker.setCurrentComponent(jobName, component);
		} catch (Exception | NoClassDefFoundError e) {
			// ignore
		}
	}
	
	
	private void mdc(final String subJobName, final String subJobPidPrefix) {
		mdcInfo.forEach(org.slf4j.MDC::put);
		org.slf4j.MDC.put("_subJobName", subJobName);
		org.slf4j.MDC.put("_subJobPid", subJobPidPrefix + subJobPidCounter.getAndIncrement());
	}
	
	
	private void sh(final String componentId) {
		ok_Hash.put(componentId, false);
		start_Hash.put(componentId, System.currentTimeMillis());
	}

	{
	s("none");
	}

	
	private String cLabel =  null;
	
		private final java.util.Map<String, Object> globalMap = new java.util.HashMap<String, Object>();
        private final static java.util.Map<String, Object> junitGlobalMap = new java.util.HashMap<String, Object>();
	
		private final java.util.Map<String, Long> start_Hash = new java.util.HashMap<String, Long>();
		private final java.util.Map<String, Long> end_Hash = new java.util.HashMap<String, Long>();
		private final java.util.Map<String, Boolean> ok_Hash = new java.util.HashMap<String, Boolean>();
		public  final java.util.List<String[]> globalBuffer = new java.util.ArrayList<String[]>();
	

private final JobStructureCatcherUtils talendJobLog = new JobStructureCatcherUtils(jobName, "ID", "0.1");
private org.talend.job.audit.JobAuditLogger auditLogger_talendJobLog = null;

private RunStat runStat = new RunStat(talendJobLog, System.getProperty("audit.interval"));

	// OSGi DataSource
	private final static String KEY_DB_DATASOURCES = "KEY_DB_DATASOURCES";
	
	private final static String KEY_DB_DATASOURCES_RAW = "KEY_DB_DATASOURCES_RAW";

	public void setDataSources(java.util.Map<String, javax.sql.DataSource> dataSources) {
		java.util.Map<String, routines.system.TalendDataSource> talendDataSources = new java.util.HashMap<String, routines.system.TalendDataSource>();
		for (java.util.Map.Entry<String, javax.sql.DataSource> dataSourceEntry : dataSources.entrySet()) {
			talendDataSources.put(dataSourceEntry.getKey(), new routines.system.TalendDataSource(dataSourceEntry.getValue()));
		}
		globalMap.put(KEY_DB_DATASOURCES, talendDataSources);
		globalMap.put(KEY_DB_DATASOURCES_RAW, new java.util.HashMap<String, javax.sql.DataSource>(dataSources));
	}
	
	public void setDataSourceReferences(List serviceReferences) throws Exception{
		
		java.util.Map<String, routines.system.TalendDataSource> talendDataSources = new java.util.HashMap<String, routines.system.TalendDataSource>();
		java.util.Map<String, javax.sql.DataSource> dataSources = new java.util.HashMap<String, javax.sql.DataSource>();
		
		for (java.util.Map.Entry<String, javax.sql.DataSource> entry : BundleUtils.getServices(serviceReferences,  javax.sql.DataSource.class).entrySet()) {
                    dataSources.put(entry.getKey(), entry.getValue());
                    talendDataSources.put(entry.getKey(), new routines.system.TalendDataSource(entry.getValue()));
		}

		globalMap.put(KEY_DB_DATASOURCES, talendDataSources);
		globalMap.put(KEY_DB_DATASOURCES_RAW, new java.util.HashMap<String, javax.sql.DataSource>(dataSources));
	}


private final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
private final java.io.PrintStream errorMessagePS = new java.io.PrintStream(new java.io.BufferedOutputStream(baos));

public String getExceptionStackTrace() {
	if ("failure".equals(this.getStatus())) {
		errorMessagePS.flush();
		return baos.toString();
	}
	return null;
}

private Exception exception;

public Exception getException() {
	if ("failure".equals(this.getStatus())) {
		return this.exception;
	}
	return null;
}

private class TalendException extends Exception {

	private static final long serialVersionUID = 1L;

	private java.util.Map<String, Object> globalMap = null;
	private Exception e = null;
	
	private String currentComponent = null;
	private String cLabel =  null;
	
	private String virtualComponentName = null;
	
	public void setVirtualComponentName (String virtualComponentName){
		this.virtualComponentName = virtualComponentName;
	}

	private TalendException(Exception e, String errorComponent, final java.util.Map<String, Object> globalMap) {
		this.currentComponent= errorComponent;
		this.globalMap = globalMap;
		this.e = e;
	}
	
	private TalendException(Exception e, String errorComponent, String errorComponentLabel, final java.util.Map<String, Object> globalMap) {
		this(e, errorComponent, globalMap);
		this.cLabel = errorComponentLabel;
	}

	public Exception getException() {
		return this.e;
	}

	public String getCurrentComponent() {
		return this.currentComponent;
	}

	
    public String getExceptionCauseMessage(Exception e){
        Throwable cause = e;
        String message = null;
        int i = 10;
        while (null != cause && 0 < i--) {
            message = cause.getMessage();
            if (null == message) {
                cause = cause.getCause();
            } else {
                break;          
            }
        }
        if (null == message) {
            message = e.getClass().getName();
        }   
        return message;
    }

	@Override
	public void printStackTrace() {
		if (!(e instanceof TalendException || e instanceof TDieException)) {
			if(virtualComponentName!=null && currentComponent.indexOf(virtualComponentName+"_")==0){
				globalMap.put(virtualComponentName+"_ERROR_MESSAGE",getExceptionCauseMessage(e));
			}
			globalMap.put(currentComponent+"_ERROR_MESSAGE",getExceptionCauseMessage(e));
			System.err.println("Exception in component " + currentComponent + " (" + jobName + ")");
		}
		if (!(e instanceof TDieException)) {
			if(e instanceof TalendException){
				e.printStackTrace();
			} else {
				e.printStackTrace();
				e.printStackTrace(errorMessagePS);
			}
		}
		if (!(e instanceof TalendException)) {
			MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE.this.exception = e;
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE.this, new Object[] { e , currentComponent, globalMap});
					break;
				}
			}

			if(!(e instanceof TDieException)){
		if(enableLogStash) {
			talendJobLog.addJobExceptionMessage(currentComponent, cLabel, null, e);
			talendJobLogProcess(globalMap);
		}
			}
		} catch (Exception e) {
			this.e.printStackTrace();
		}
		}
	}
}

			public void tFixedFlowInput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFixedFlowInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tJavaRow_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFixedFlowInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void talendJobLog_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					talendJobLog_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tFixedFlowInput_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void talendJobLog_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	














public static class row3Struct implements routines.system.IPersistableRow<row3Struct> {
    final static byte[] commonByteArrayLock_MECO_CIS_POC_MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE = new byte[0];
    static byte[] commonByteArray_MECO_CIS_POC_MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE = new byte[0];

	
			    public Integer target_count;

				public Integer getTarget_count () {
					return this.target_count;
				}

				public Boolean target_countIsNullable(){
				    return true;
				}
				public Boolean target_countIsKey(){
				    return false;
				}
				public Integer target_countLength(){
				    return null;
				}
				public Integer target_countPrecision(){
				    return null;
				}
				public String target_countDefault(){
				
					return null;
				
				}
				public String target_countComment(){
				
				    return "";
				
				}
				public String target_countPattern(){
				
					return "";
				
				}
				public String target_countOriginalDbColumnName(){
				
					return "target_count";
				
				}

				


	private Integer readInteger(ObjectInputStream dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}
	
	private Integer readInteger(org.jboss.marshalling.Unmarshaller dis) throws IOException{
		Integer intReturn;
        int length = 0;
        length = dis.readByte();
		if (length == -1) {
			intReturn = null;
		} else {
	    	intReturn = dis.readInt();
		}
		return intReturn;
	}

	private void writeInteger(Integer intNum, ObjectOutputStream dos) throws IOException{
		if(intNum == null) {
            dos.writeByte(-1);
		} else {
			dos.writeByte(0);
	    	dos.writeInt(intNum);
    	}
	}
	
	private void writeInteger(Integer intNum, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(intNum == null) {
			marshaller.writeByte(-1);
		} else {
			marshaller.writeByte(0);
			marshaller.writeInt(intNum);
    	}
	}

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_MECO_CIS_POC_MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE) {

        	try {

        		int length = 0;
		
						this.target_count = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_MECO_CIS_POC_MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE) {

        	try {

        		int length = 0;
		
						this.target_count = readInteger(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// Integer
				
						writeInteger(this.target_count,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// Integer
				
						writeInteger(this.target_count,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("target_count="+String.valueOf(target_count));
	    sb.append("]");

	    return sb.toString();
    }
        public String toLogString(){
        	StringBuilder sb = new StringBuilder();
        	
        				if(target_count == null){
        					sb.append("<null>");
        				}else{
            				sb.append(target_count);
            			}
            		
        			sb.append("|");
        		
        	return sb.toString();
        }

    /**
     * Compare keys
     */
    public int compareTo(row3Struct other) {

		int returnValue = -1;
		
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}

public void tFixedFlowInput_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tFixedFlowInput_1_SUBPROCESS_STATE", 0);

	final boolean execStat = this.execStat;

		mdc("tFixedFlowInput_1", "FVlwba_");

	
		String iterateId = "";
	
	
	String currentComponent = "";
	s("none");
	String cLabel =  null;
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row3Struct row3 = new row3Struct();




	
	/**
	 * [tJavaRow_2 begin ] start
	 */

	

	
		
		sh("tJavaRow_2");
		
	
	s(currentComponent="tJavaRow_2");
	
			
			
	
			cLabel="tJavaRow_1";
		
					if(enableLogStash) {
						runStat.log(resourceMap,iterateId,0,0,"row3");
					}
				
		int tos_count_tJavaRow_2 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tJavaRow_2", "tJavaRow_1", "tJavaRow");
				talendJobLogProcess(globalMap);
				s(currentComponent);
			}
			

int nb_line_tJavaRow_2 = 0;

 



		

/**
 * [tJavaRow_2 begin ] stop
 */




	
	/**
	 * [tFixedFlowInput_1 begin ] start
	 */

	

	
		
		sh("tFixedFlowInput_1");
		
	
	s(currentComponent="tFixedFlowInput_1");
	
			
			
	
		int tos_count_tFixedFlowInput_1 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tFixedFlowInput_1", "tFixedFlowInput_1", "tFixedFlowInput");
				talendJobLogProcess(globalMap);
				s(currentComponent);
			}
			


StringBuilder result_tFixedFlowInput_1 = new StringBuilder();
	result_tFixedFlowInput_1.append("YWJjCmNiYQ==");
String originalFileContent_tFixedFlowInput_1 = "";
try {
    originalFileContent_tFixedFlowInput_1 = new String(java.util.Base64.getMimeDecoder().decode(result_tFixedFlowInput_1.toString()), utf8Charset);
} catch (java.lang.Exception e) {
globalMap.put("tFixedFlowInput_1_ERROR_MESSAGE",e.getMessage());
    e.printStackTrace();
}

int nb_line_tFixedFlowInput_1 = 0;

for (int i_tFixedFlowInput_1 = 0 ; i_tFixedFlowInput_1 < 10 ; i_tFixedFlowInput_1++) {	

java.io.InputStream ins_tFixedFlowInput_1 = new java.io.ByteArrayInputStream(originalFileContent_tFixedFlowInput_1.getBytes(utf8Charset));        
org.talend.fileprocess.FileInputDelimited fid_tFixedFlowInput_1 = new org.talend.fileprocess.FileInputDelimited(ins_tFixedFlowInput_1, utf8Charset,";","\n",true, 0, 0, -1, -1, false);

while (fid_tFixedFlowInput_1.nextRecord()) {
	nb_line_tFixedFlowInput_1++;
			row3 = new row3Struct();

				if(0 < fid_tFixedFlowInput_1.getColumnsCountOfCurrentRow()){
					String colContent = fid_tFixedFlowInput_1.get(0);				
                    		row3.target_count = (colContent == null || colContent.trim().length() == 0) ? null : ParserUtils.parseTo_Integer(colContent);			
				} else { 
					
					row3.target_count = null;
				
				}
			
			

 



		

/**
 * [tFixedFlowInput_1 begin ] stop
 */

	
	/**
	 * [tFixedFlowInput_1 main ] start
	 */

	

	
	
	s(currentComponent="tFixedFlowInput_1");
	
			
			
	

 


	tos_count_tFixedFlowInput_1++;

		

/**
 * [tFixedFlowInput_1 main ] stop
 */

	
	/**
	 * [tFixedFlowInput_1 process_data_begin ] start
	 */

	

	
	
	s(currentComponent="tFixedFlowInput_1");
	
			
			
	

 



		

/**
 * [tFixedFlowInput_1 process_data_begin ] stop
 */


	
	/**
	 * [tJavaRow_2 main ] start
	 */

	

	
	
	s(currentComponent="tJavaRow_2");
	
			
			
	
			cLabel="tJavaRow_1";
		
					if(enableLogStash) {
						if(runStat.updateLog(iterateId,1,1
						
							,"row3","tFixedFlowInput_1","tFixedFlowInput_1","tFixedFlowInput","tJavaRow_2","tJavaRow_1","tJavaRow"
						
						)) {
							talendJobLogProcess(globalMap);
						}
					}
					
    			if(log.isTraceEnabled()){
    				log.trace("row3 - " + (row3==null? "": row3.toLogString()));
    			}
    		

    
MemoCode__Row_input_row input_row = new MemoCode__Row_input_row();



// __studio_memo_code__ CODE begin__

{}

if (input_row.target_count != null) {
	 context.target_count = input_row.target_count;
	}
	//Optional: Print to console for debugging
System.out.println("Target Count captured in Context: " + context.target_count);
// __studio_memo_code__ CODE end__
    nb_line_tJavaRow_2++;   

 


	tos_count_tJavaRow_2++;

		

/**
 * [tJavaRow_2 main ] stop
 */

	
	/**
	 * [tJavaRow_2 process_data_begin ] start
	 */

	

	
	
	s(currentComponent="tJavaRow_2");
	
			
			
	
			cLabel="tJavaRow_1";
		

 



		

/**
 * [tJavaRow_2 process_data_begin ] stop
 */

	
	/**
	 * [tJavaRow_2 process_data_end ] start
	 */

	

	
	
	s(currentComponent="tJavaRow_2");
	
			
			
	
			cLabel="tJavaRow_1";
		

 



		

/**
 * [tJavaRow_2 process_data_end ] stop
 */




	
	/**
	 * [tFixedFlowInput_1 process_data_end ] start
	 */

	

	
	
	s(currentComponent="tFixedFlowInput_1");
	
			
			
	

 



		

/**
 * [tFixedFlowInput_1 process_data_end ] stop
 */

	
	/**
	 * [tFixedFlowInput_1 end ] start
	 */

	

	
	
	s(currentComponent="tFixedFlowInput_1");
	
			
			
	

		}
	  fid_tFixedFlowInput_1.close();
	}

	globalMap.put("tFixedFlowInput_1_NB_LINE", nb_line_tFixedFlowInput_1);

 

ok_Hash.put("tFixedFlowInput_1", true);
end_Hash.put("tFixedFlowInput_1", System.currentTimeMillis());




		

/**
 * [tFixedFlowInput_1 end ] stop
 */


	
	/**
	 * [tJavaRow_2 end ] start
	 */

	

	
	
	s(currentComponent="tJavaRow_2");
	
			
			
	
			cLabel="tJavaRow_1";
		

globalMap.put("tJavaRow_2_NB_LINE",nb_line_tJavaRow_2);
				if(enableLogStash) {
				
				 		if(runStat.log(resourceMap,iterateId,"row3",2,0,
				 			"tFixedFlowInput_1","tFixedFlowInput_1","tFixedFlowInput","tJavaRow_2","tJavaRow_1","tJavaRow","output")) {
							talendJobLogProcess(globalMap);
						}
					
			  	}
			  	
 

ok_Hash.put("tJavaRow_2", true);
end_Hash.put("tJavaRow_2", System.currentTimeMillis());




		

/**
 * [tJavaRow_2 end ] stop
 */




				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if (!(e instanceof TalendException) && !(e instanceof TDieException)) {
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, cLabel, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tFixedFlowInput_1 finally ] start
	 */

	

	
	
	s(currentComponent="tFixedFlowInput_1");
	
			
			
	

 



		

/**
 * [tFixedFlowInput_1 finally ] stop
 */


	
	/**
	 * [tJavaRow_2 finally ] start
	 */

	

	
	
	s(currentComponent="tJavaRow_2");
	
			
			
	
			cLabel="tJavaRow_1";
		

 



		

/**
 * [tJavaRow_2 finally ] stop
 */




				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tFixedFlowInput_1_SUBPROCESS_STATE", 1);
	}
	


public void talendJobLogProcess(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("talendJobLog_SUBPROCESS_STATE", 0);

	final boolean execStat = this.execStat;


	
		String iterateId = "";
	
	
	String currentComponent = "";
	s("none");
	String cLabel =  null;
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;





	
	/**
	 * [talendJobLog begin ] start
	 */

	

	
		
		sh("talendJobLog");
		
	
	s(currentComponent="talendJobLog");
	
			
			
	
		int tos_count_talendJobLog = 0;
		

	for (JobStructureCatcherUtils.JobStructureCatcherMessage jcm : talendJobLog.getMessages()) {
		org.talend.job.audit.JobContextBuilder builder_talendJobLog = org.talend.job.audit.JobContextBuilder.create().jobName(jcm.job_name).jobId(jcm.job_id).jobVersion(jcm.job_version)
			.custom("process_id", jcm.pid).custom("thread_id", jcm.tid).custom("pid", pid).custom("father_pid", fatherPid).custom("root_pid", rootPid);
		org.talend.logging.audit.Context log_context_talendJobLog = null;
		
		
		if(jcm.log_type == JobStructureCatcherUtils.LogType.PERFORMANCE){
			long timeMS = jcm.end_time - jcm.start_time;
			String duration = String.valueOf(timeMS);
			
			log_context_talendJobLog = builder_talendJobLog
				.sourceId(jcm.sourceId).sourceLabel(jcm.sourceLabel).sourceConnectorType(jcm.sourceComponentName)
				.targetId(jcm.targetId).targetLabel(jcm.targetLabel).targetConnectorType(jcm.targetComponentName)
				.connectionName(jcm.current_connector).rows(jcm.row_count).duration(duration).build();
			auditLogger_talendJobLog.flowExecution(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.JOBSTART) {
			log_context_talendJobLog = builder_talendJobLog.timestamp(jcm.moment).build();
			auditLogger_talendJobLog.jobstart(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.JOBEND) {
			long timeMS = jcm.end_time - jcm.start_time;
			String duration = String.valueOf(timeMS);
		
			log_context_talendJobLog = builder_talendJobLog
				.timestamp(jcm.moment).duration(duration).status(jcm.status).build();
			auditLogger_talendJobLog.jobstop(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.RUNCOMPONENT) {
			log_context_talendJobLog = builder_talendJobLog.timestamp(jcm.moment)
				.connectorType(jcm.component_name).connectorId(jcm.component_id).connectorLabel(jcm.component_label).build();
			auditLogger_talendJobLog.runcomponent(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.FLOWINPUT) {//log current component input line
			long timeMS = jcm.end_time - jcm.start_time;
			String duration = String.valueOf(timeMS);
			
			log_context_talendJobLog = builder_talendJobLog
				.connectorType(jcm.component_name).connectorId(jcm.component_id).connectorLabel(jcm.component_label)
				.connectionName(jcm.current_connector).connectionType(jcm.current_connector_type)
				.rows(jcm.total_row_number).duration(duration).build();
			auditLogger_talendJobLog.flowInput(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.FLOWOUTPUT) {//log current component output/reject line
			long timeMS = jcm.end_time - jcm.start_time;
			String duration = String.valueOf(timeMS);
			
			log_context_talendJobLog = builder_talendJobLog
				.connectorType(jcm.component_name).connectorId(jcm.component_id).connectorLabel(jcm.component_label)
				.connectionName(jcm.current_connector).connectionType(jcm.current_connector_type)
				.rows(jcm.total_row_number).duration(duration).build();
			auditLogger_talendJobLog.flowOutput(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.JOBERROR) {
			java.lang.Exception e_talendJobLog = jcm.exception;
			if(e_talendJobLog!=null) {
				try(java.io.StringWriter sw_talendJobLog = new java.io.StringWriter();java.io.PrintWriter pw_talendJobLog = new java.io.PrintWriter(sw_talendJobLog)) {
					e_talendJobLog.printStackTrace(pw_talendJobLog);
					builder_talendJobLog.custom("stacktrace", sw_talendJobLog.getBuffer().substring(0,java.lang.Math.min(sw_talendJobLog.getBuffer().length(), 512)));
				}
			}

			if(jcm.extra_info!=null) {
				builder_talendJobLog.connectorId(jcm.component_id).custom("extra_info", jcm.extra_info);
			}
				
			log_context_talendJobLog = builder_talendJobLog
				.connectorType(jcm.component_id.substring(0, jcm.component_id.lastIndexOf('_')))
				.connectorId(jcm.component_id)
				.connectorLabel(jcm.component_label == null ? jcm.component_id : jcm.component_label).build();

			auditLogger_talendJobLog.exception(log_context_talendJobLog);
		}
		
		
		
	}

 



		

/**
 * [talendJobLog begin ] stop
 */

	
	/**
	 * [talendJobLog main ] start
	 */

	

	
	
	s(currentComponent="talendJobLog");
	
			
			
	

 


	tos_count_talendJobLog++;

		

/**
 * [talendJobLog main ] stop
 */

	
	/**
	 * [talendJobLog process_data_begin ] start
	 */

	

	
	
	s(currentComponent="talendJobLog");
	
			
			
	

 



		

/**
 * [talendJobLog process_data_begin ] stop
 */

	
	/**
	 * [talendJobLog process_data_end ] start
	 */

	

	
	
	s(currentComponent="talendJobLog");
	
			
			
	

 



		

/**
 * [talendJobLog process_data_end ] stop
 */

	
	/**
	 * [talendJobLog end ] start
	 */

	

	
	
	s(currentComponent="talendJobLog");
	
			
			
	

 

ok_Hash.put("talendJobLog", true);
end_Hash.put("talendJobLog", System.currentTimeMillis());




		

/**
 * [talendJobLog end ] stop
 */

				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if (!(e instanceof TalendException) && !(e instanceof TDieException)) {
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, cLabel, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [talendJobLog finally ] start
	 */

	

	
	
	s(currentComponent="talendJobLog");
	
			
			
	

 



		

/**
 * [talendJobLog finally ] stop
 */

				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("talendJobLog_SUBPROCESS_STATE", 1);
	}
	
    public String resuming_logs_dir_path = null;
    public String resuming_checkpoint_path = null;
    public String parent_part_launcher = null;
    private String resumeEntryMethodName = null;
    private boolean globalResumeTicket = false;

    public boolean watch = false;
    // portStats is null, it means don't execute the statistics
    public Integer portStats = null;
    public int portTraces = 4334;
    public String clientHost;
    public String defaultClientHost = "localhost";
    public String contextStr = "Default";
    public boolean isDefaultContext = true;
    public String pid = "0";
    public String rootPid = null;
    public String fatherPid = null;
    public String fatherNode = null;
    public long startTime = 0;
    public boolean isChildJob = false;
    public String log4jLevel = "";
    
    private boolean enableLogStash;
    private boolean enableLineage;

    private boolean execStat = true;
    
    private ThreadLocal<java.util.Map<String, String>> threadLocal = new ThreadLocal<java.util.Map<String, String>>() {
        protected java.util.Map<String, String> initialValue() {
            java.util.Map<String,String> threadRunResultMap = new java.util.HashMap<String, String>();
            threadRunResultMap.put("errorCode", null);
            threadRunResultMap.put("status", "");
            return threadRunResultMap;
        };
    };


    protected PropertiesWithType context_param = new PropertiesWithType();
    public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();

    public String status= "";
    
    
    private final static java.util.Properties jobInfo = new java.util.Properties();
    private final static java.util.Map<String,String> mdcInfo = new java.util.HashMap<>();
    private final static java.util.concurrent.atomic.AtomicLong subJobPidCounter = new java.util.concurrent.atomic.AtomicLong();


    public static void main(String[] args){
        final MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODEClass = new MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE();

        int exitCode = MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODEClass.runJobInTOS(args);
	        if(exitCode==0){
		        log.info("TalendJob: 'MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE' - Done.");
	        }

        System.exit(exitCode);
    }
	

	
	
	private void getjobInfo() {
		final String TEMPLATE_PATH = "src/main/templates/jobInfo_template.properties";
		final String BUILD_PATH = "../jobInfo.properties";
		final String path = this.getClass().getResource("").getPath();
		if(path.lastIndexOf("target") > 0) {
			final java.io.File templateFile = new java.io.File(
					path.substring(0, path.lastIndexOf("target")).concat(TEMPLATE_PATH));
			if (templateFile.exists()) {
				readJobInfo(templateFile);
				return;
			}
		}
			readJobInfo(new java.io.File(BUILD_PATH));
	}

    private void readJobInfo(java.io.File jobInfoFile){
	
        if(jobInfoFile.exists()) {
            try (java.io.InputStream is = new java.io.FileInputStream(jobInfoFile)) {
            	jobInfo.load(is);
            } catch (IOException e) {
            	 
                log.debug("Read jobInfo.properties file fail: " + e.getMessage());
                
            }
        }
		log.info(String.format("Project name: %s\tJob name: %s\tGIT Commit ID: %s\tTalend Version: %s",
				projectName,jobName,jobInfo.getProperty("gitCommitId"), "8.0.1.20260102_0846-patch"));
		
    }


    public String[][] runJob(String[] args) {

        int exitCode = runJobInTOS(args);
        String[][] bufferValue = new String[][] { { Integer.toString(exitCode) } };

        return bufferValue;
    }

    public boolean hastBufferOutputComponent() {
		boolean hastBufferOutput = false;
    	
        return hastBufferOutput;
    }

    public int runJobInTOS(String[] args) {
	   	// reset status
	   	status = "";
	   	
        String lastStr = "";
        for (String arg : args) {
            if (arg.equalsIgnoreCase("--context_param")) {
                lastStr = arg;
            } else if (lastStr.equals("")) {
                evalParam(arg);
            } else {
                evalParam(lastStr + " " + arg);
                lastStr = "";
            }
        }

        final boolean enableCBP = false;
        boolean inOSGi = routines.system.BundleUtils.inOSGi();

        boolean needSendForCBP = false;
        if (!inOSGi && isCBPClientPresent) {
        if(org.talend.metrics.CBPClient.getInstanceForCurrentVM() == null) {
            try {
                needSendForCBP = true;
                org.talend.metrics.CBPClient.startListenIfNotStarted(enableCBP, true);
            } catch (java.lang.Exception e) {
                errorCode = 1;
                status = "failure";
                e.printStackTrace();
                return 1;
            }
        }
        }
        
        enableLogStash = "true".equalsIgnoreCase(System.getProperty("audit.enabled"));

	        if(!"".equals(log4jLevel)){
	        	
				
				
				if("trace".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.TRACE);
				}else if("debug".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.DEBUG);
				}else if("info".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.INFO);
				}else if("warn".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.WARN);
				}else if("error".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.ERROR);
				}else if("fatal".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.FATAL);
				}else if ("off".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.OFF);
				}
				org.apache.logging.log4j.core.config.Configurator.setLevel(org.apache.logging.log4j.LogManager.getRootLogger().getName(), log.getLevel());
				
			}

	        getjobInfo();
			log.info("TalendJob: 'MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE' - Start.");
		

                java.util.Set<Object> jobInfoKeys = jobInfo.keySet();
                for(Object jobInfoKey: jobInfoKeys) {
                    org.slf4j.MDC.put("_" + jobInfoKey.toString(), jobInfo.get(jobInfoKey).toString());
                }
                org.slf4j.MDC.put("_pid", pid);
                org.slf4j.MDC.put("_rootPid", rootPid);
                org.slf4j.MDC.put("_fatherPid", fatherPid);
                org.slf4j.MDC.put("_projectName", projectName);
                org.slf4j.MDC.put("_startTimestamp",java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC ).format( java.time.format.DateTimeFormatter.ISO_INSTANT ));
                org.slf4j.MDC.put("_jobRepositoryId","ID");
                org.slf4j.MDC.put("_compiledAtTimestamp","2026-05-18T05:58:53.045489300Z");

                java.lang.management.RuntimeMXBean mx = java.lang.management.ManagementFactory.getRuntimeMXBean();
                String[] mxNameTable = mx.getName().split("@"); //$NON-NLS-1$
                if (mxNameTable.length == 2) {
                    org.slf4j.MDC.put("_systemPid", mxNameTable[0]);
                } else {
                    org.slf4j.MDC.put("_systemPid", String.valueOf(java.lang.Thread.currentThread().getId()));
                }

		
		
			if(enableLogStash) {
				java.util.Properties properties_talendJobLog = new java.util.Properties();
				properties_talendJobLog.setProperty("root.logger", "audit");
				properties_talendJobLog.setProperty("encoding", "UTF-8");
				properties_talendJobLog.setProperty("application.name", "Talend Studio");
				properties_talendJobLog.setProperty("service.name", "Talend Studio Job");
				properties_talendJobLog.setProperty("instance.name", "Talend Studio Job Instance");
				properties_talendJobLog.setProperty("propagate.appender.exceptions", "none");
				properties_talendJobLog.setProperty("log.appender", "file");
				properties_talendJobLog.setProperty("appender.file.path", "audit.json");
				properties_talendJobLog.setProperty("appender.file.maxsize", "52428800");
				properties_talendJobLog.setProperty("appender.file.maxbackup", "20");
				properties_talendJobLog.setProperty("host", "false");

				System.getProperties().stringPropertyNames().stream()
					.filter(it -> it.startsWith("audit.logger."))
					.forEach(key -> properties_talendJobLog.setProperty(key.substring("audit.logger.".length()), System.getProperty(key)));

				
				
				
				org.apache.logging.log4j.core.config.Configurator.setLevel(properties_talendJobLog.getProperty("root.logger"), org.apache.logging.log4j.Level.DEBUG);
				
				auditLogger_talendJobLog = org.talend.job.audit.JobEventAuditLoggerFactory.createJobAuditLogger(properties_talendJobLog);
			}
		

        if(clientHost == null) {
            clientHost = defaultClientHost;
        }

        if(pid == null || "0".equals(pid)) {
            pid = TalendString.getAsciiRandomString(6);
        }

            org.slf4j.MDC.put("_pid", pid);

        if (rootPid==null) {
            rootPid = pid;
        }

            org.slf4j.MDC.put("_rootPid", rootPid);

        if (fatherPid==null) {
            fatherPid = pid;
        }else{
            isChildJob = true;
        }
            org.slf4j.MDC.put("_fatherPid", fatherPid);


        try {
            java.util.Dictionary<String, Object> jobProperties = null;
            if (inOSGi) {
                jobProperties = routines.system.BundleUtils.getJobProperties(jobName);
    
                if (jobProperties != null && jobProperties.get("context") != null) {
                    contextStr = (String)jobProperties.get("context");
                }

                if (jobProperties != null && jobProperties.get("taskExecutionId") != null) {
                    taskExecutionId = (String)jobProperties.get("taskExecutionId");
                }

                // extract ids from parent route
                if(null == taskExecutionId || taskExecutionId.isEmpty()){
                    for(String arg : args) {
                        if(arg.startsWith("--context_param")
                                && (arg.contains("taskExecutionId") || arg.contains("jobExecutionId"))){

                            String keyValue = arg.replace("--context_param", "");
                            String[] parts = keyValue.split("=");
                            String[] cleanParts = java.util.Arrays.stream(parts)
                                    .filter(s -> !s.isEmpty())
                                    .toArray(String[]::new);
                            if (cleanParts.length == 2) {
                                String key = cleanParts[0];
                                String value = cleanParts[1];
                                if ("taskExecutionId".equals(key.trim()) && null != value) {
                                    taskExecutionId = value.trim();
                                }else if ("jobExecutionId".equals(key.trim()) && null != value) {
                                    jobExecutionId = value.trim();
                                }
                            }
                        }
                    }
                }
            }

            // first load default key-value pairs from application.properties
            if(isStandaloneMS) {
                context.putAll(this.getDefaultProperties());
            }
            //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead.
            java.io.InputStream inContext = MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE.class.getClassLoader().getResourceAsStream("meco_cis_poc/memocode_job_csv_accounts_ingestion_tjavarow_2_code_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
            }
            if (inContext != null) {
                try {
                    //defaultProps is in order to keep the original context value
                    if(context != null && context.isEmpty()) {
    	                defaultProps.load(inContext);
    	                if (inOSGi && jobProperties != null) {
                             java.util.Enumeration<String> keys = jobProperties.keys();
                             while (keys.hasMoreElements()) {
                                 String propKey = keys.nextElement();
                                 if (defaultProps.containsKey(propKey)) {
                                     defaultProps.put(propKey, (String) jobProperties.get(propKey));
                                 }
                             }
    	                }
    	                context = new ContextProperties(defaultProps);
                    }
                    if(isStandaloneMS) {
                        // override context key-value pairs if provided using --context=contextName
                        defaultProps.load(inContext);
                        context.putAll(defaultProps);
                    }
                } finally {
                    inContext.close();
                }
            } else if (!isDefaultContext) {
                //print info and job continue to run, for case: context_param is not empty.
                System.err.println("Could not find the context " + contextStr);
            }
            // override key-value pairs if provided via --config.location=file1.file2 OR --config.additional-location=file1,file2
            if(isStandaloneMS) {
                context.putAll(this.getAdditionalProperties());
            }
            
            // override key-value pairs if provide via command line like --key1=value1,--key2=value2
            if(!context_param.isEmpty()) {
                context.putAll(context_param);
				//set types for params from parentJobs
				for (Object key: context_param.keySet()){
					String context_key = key.toString();
					String context_type = context_param.getContextType(context_key);
					context.setContextType(context_key, context_type);

				}
            }
            class ContextProcessing {
                private void processContext_0() {
                        context.setContextType("conn_hive_target_table_AdditionalJDBCParameters", "id_String");
                        if(context.getStringValue("conn_hive_target_table_AdditionalJDBCParameters") == null) {
                            context.conn_hive_target_table_AdditionalJDBCParameters = null;
                        } else {
                            context.conn_hive_target_table_AdditionalJDBCParameters=(String) context.getProperty("conn_hive_target_table_AdditionalJDBCParameters");
                        }
                        context.setContextType("conn_hive_target_table_Database", "id_String");
                        if(context.getStringValue("conn_hive_target_table_Database") == null) {
                            context.conn_hive_target_table_Database = null;
                        } else {
                            context.conn_hive_target_table_Database=(String) context.getProperty("conn_hive_target_table_Database");
                        }
                        context.setContextType("conn_hive_target_table_Password", "id_Password");
                        if(context.getStringValue("conn_hive_target_table_Password") == null) {
                            context.conn_hive_target_table_Password = null;
                        } else {
                            String pwd_conn_hive_target_table_Password_value = context.getProperty("conn_hive_target_table_Password");
                            context.conn_hive_target_table_Password = null;
                            if(pwd_conn_hive_target_table_Password_value!=null) {
                                if(context_param.containsKey("conn_hive_target_table_Password")) {//no need to decrypt if it come from program argument or parent job runtime
                                    context.conn_hive_target_table_Password = pwd_conn_hive_target_table_Password_value;
                                } else if (!pwd_conn_hive_target_table_Password_value.isEmpty()) {
                                    try {
                                        context.conn_hive_target_table_Password = routines.system.PasswordEncryptUtil.decryptPassword(pwd_conn_hive_target_table_Password_value);
                                        context.put("conn_hive_target_table_Password",context.conn_hive_target_table_Password);
                                    } catch (java.lang.RuntimeException e) {
                                        //do nothing
                                    }
                                }
                            }
                        }
                        context.setContextType("conn_hive_target_table_Server", "id_String");
                        if(context.getStringValue("conn_hive_target_table_Server") == null) {
                            context.conn_hive_target_table_Server = null;
                        } else {
                            context.conn_hive_target_table_Server=(String) context.getProperty("conn_hive_target_table_Server");
                        }
                        context.setContextType("conn_hive_target_table_HiveMetastorePort", "id_String");
                        if(context.getStringValue("conn_hive_target_table_HiveMetastorePort") == null) {
                            context.conn_hive_target_table_HiveMetastorePort = null;
                        } else {
                            context.conn_hive_target_table_HiveMetastorePort=(String) context.getProperty("conn_hive_target_table_HiveMetastorePort");
                        }
                        context.setContextType("conn_hive_target_table_Port", "id_String");
                        if(context.getStringValue("conn_hive_target_table_Port") == null) {
                            context.conn_hive_target_table_Port = null;
                        } else {
                            context.conn_hive_target_table_Port=(String) context.getProperty("conn_hive_target_table_Port");
                        }
                        context.setContextType("conn_hive_target_table_Login", "id_String");
                        if(context.getStringValue("conn_hive_target_table_Login") == null) {
                            context.conn_hive_target_table_Login = null;
                        } else {
                            context.conn_hive_target_table_Login=(String) context.getProperty("conn_hive_target_table_Login");
                        }
                        context.setContextType("conn_sftp_mbtc_username", "id_String");
                        if(context.getStringValue("conn_sftp_mbtc_username") == null) {
                            context.conn_sftp_mbtc_username = null;
                        } else {
                            context.conn_sftp_mbtc_username=(String) context.getProperty("conn_sftp_mbtc_username");
                        }
                        context.setContextType("conn_sftp_mbtc_host", "id_String");
                        if(context.getStringValue("conn_sftp_mbtc_host") == null) {
                            context.conn_sftp_mbtc_host = null;
                        } else {
                            context.conn_sftp_mbtc_host=(String) context.getProperty("conn_sftp_mbtc_host");
                        }
                        context.setContextType("conn_sftp_mbtc_port", "id_Integer");
                        if(context.getStringValue("conn_sftp_mbtc_port") == null) {
                            context.conn_sftp_mbtc_port = null;
                        } else {
                            try{
                                context.conn_sftp_mbtc_port=routines.system.ParserUtils.parseTo_Integer (context.getProperty("conn_sftp_mbtc_port"));
                            } catch(NumberFormatException e){
                                log.warn(String.format("Null value will be used for context parameter %s: %s", "conn_sftp_mbtc_port", e.getMessage()));
                                context.conn_sftp_mbtc_port=null;
                            }
                        }
                        context.setContextType("conn_sftp_mbtc_password", "id_Password");
                        if(context.getStringValue("conn_sftp_mbtc_password") == null) {
                            context.conn_sftp_mbtc_password = null;
                        } else {
                            String pwd_conn_sftp_mbtc_password_value = context.getProperty("conn_sftp_mbtc_password");
                            context.conn_sftp_mbtc_password = null;
                            if(pwd_conn_sftp_mbtc_password_value!=null) {
                                if(context_param.containsKey("conn_sftp_mbtc_password")) {//no need to decrypt if it come from program argument or parent job runtime
                                    context.conn_sftp_mbtc_password = pwd_conn_sftp_mbtc_password_value;
                                } else if (!pwd_conn_sftp_mbtc_password_value.isEmpty()) {
                                    try {
                                        context.conn_sftp_mbtc_password = routines.system.PasswordEncryptUtil.decryptPassword(pwd_conn_sftp_mbtc_password_value);
                                        context.put("conn_sftp_mbtc_password",context.conn_sftp_mbtc_password);
                                    } catch (java.lang.RuntimeException e) {
                                        //do nothing
                                    }
                                }
                            }
                        }
                        context.setContextType("conn_hadoop_cluster_User", "id_String");
                        if(context.getStringValue("conn_hadoop_cluster_User") == null) {
                            context.conn_hadoop_cluster_User = null;
                        } else {
                            context.conn_hadoop_cluster_User=(String) context.getProperty("conn_hadoop_cluster_User");
                        }
                        context.setContextType("conn_hadoop_cluster_NameNodeUri", "id_String");
                        if(context.getStringValue("conn_hadoop_cluster_NameNodeUri") == null) {
                            context.conn_hadoop_cluster_NameNodeUri = null;
                        } else {
                            context.conn_hadoop_cluster_NameNodeUri=(String) context.getProperty("conn_hadoop_cluster_NameNodeUri");
                        }
                        context.setContextType("target_count", "id_Integer");
                        if(context.getStringValue("target_count") == null) {
                            context.target_count = null;
                        } else {
                            try{
                                context.target_count=routines.system.ParserUtils.parseTo_Integer (context.getProperty("target_count"));
                            } catch(NumberFormatException e){
                                log.warn(String.format("Null value will be used for context parameter %s: %s", "target_count", e.getMessage()));
                                context.target_count=null;
                            }
                        }
                        context.setContextType("src_file_dir", "id_String");
                        if(context.getStringValue("src_file_dir") == null) {
                            context.src_file_dir = null;
                        } else {
                            context.src_file_dir=(String) context.getProperty("src_file_dir");
                        }
                        context.setContextType("filename", "id_String");
                        if(context.getStringValue("filename") == null) {
                            context.filename = null;
                        } else {
                            context.filename=(String) context.getProperty("filename");
                        }
                        context.setContextType("file_extension", "id_String");
                        if(context.getStringValue("file_extension") == null) {
                            context.file_extension = null;
                        } else {
                            context.file_extension=(String) context.getProperty("file_extension");
                        }
                        context.setContextType("hdfs_raw_src_file_dir", "id_String");
                        if(context.getStringValue("hdfs_raw_src_file_dir") == null) {
                            context.hdfs_raw_src_file_dir = null;
                        } else {
                            context.hdfs_raw_src_file_dir=(String) context.getProperty("hdfs_raw_src_file_dir");
                        }
                        context.setContextType("source_schema", "id_String");
                        if(context.getStringValue("source_schema") == null) {
                            context.source_schema = null;
                        } else {
                            context.source_schema=(String) context.getProperty("source_schema");
                        }
                        context.setContextType("target_schema", "id_String");
                        if(context.getStringValue("target_schema") == null) {
                            context.target_schema = null;
                        } else {
                            context.target_schema=(String) context.getProperty("target_schema");
                        }
                        context.setContextType("target_tablename", "id_String");
                        if(context.getStringValue("target_tablename") == null) {
                            context.target_tablename = null;
                        } else {
                            context.target_tablename=(String) context.getProperty("target_tablename");
                        }
                        context.setContextType("target_table_location", "id_String");
                        if(context.getStringValue("target_table_location") == null) {
                            context.target_table_location = null;
                        } else {
                            context.target_table_location=(String) context.getProperty("target_table_location");
                        }
                        context.setContextType("create_table_schema", "id_String");
                        if(context.getStringValue("create_table_schema") == null) {
                            context.create_table_schema = null;
                        } else {
                            context.create_table_schema=(String) context.getProperty("create_table_schema");
                        }
                        context.setContextType("select_columns", "id_String");
                        if(context.getStringValue("select_columns") == null) {
                            context.select_columns = null;
                        } else {
                            context.select_columns=(String) context.getProperty("select_columns");
                        }
                        context.setContextType("source_count", "id_Integer");
                        if(context.getStringValue("source_count") == null) {
                            context.source_count = null;
                        } else {
                            try{
                                context.source_count=routines.system.ParserUtils.parseTo_Integer (context.getProperty("source_count"));
                            } catch(NumberFormatException e){
                                log.warn(String.format("Null value will be used for context parameter %s: %s", "source_count", e.getMessage()));
                                context.source_count=null;
                            }
                        }
                        context.setContextType("job_server_file_dir", "id_String");
                        if(context.getStringValue("job_server_file_dir") == null) {
                            context.job_server_file_dir = null;
                        } else {
                            context.job_server_file_dir=(String) context.getProperty("job_server_file_dir");
                        }
                        context.setContextType("audit_database", "id_String");
                        if(context.getStringValue("audit_database") == null) {
                            context.audit_database = null;
                        } else {
                            context.audit_database=(String) context.getProperty("audit_database");
                        }
                        context.setContextType("manifest_table", "id_String");
                        if(context.getStringValue("manifest_table") == null) {
                            context.manifest_table = null;
                        } else {
                            context.manifest_table=(String) context.getProperty("manifest_table");
                        }
                        context.setContextType("conn_oracle_source_table_Password", "id_Password");
                        if(context.getStringValue("conn_oracle_source_table_Password") == null) {
                            context.conn_oracle_source_table_Password = null;
                        } else {
                            String pwd_conn_oracle_source_table_Password_value = context.getProperty("conn_oracle_source_table_Password");
                            context.conn_oracle_source_table_Password = null;
                            if(pwd_conn_oracle_source_table_Password_value!=null) {
                                if(context_param.containsKey("conn_oracle_source_table_Password")) {//no need to decrypt if it come from program argument or parent job runtime
                                    context.conn_oracle_source_table_Password = pwd_conn_oracle_source_table_Password_value;
                                } else if (!pwd_conn_oracle_source_table_Password_value.isEmpty()) {
                                    try {
                                        context.conn_oracle_source_table_Password = routines.system.PasswordEncryptUtil.decryptPassword(pwd_conn_oracle_source_table_Password_value);
                                        context.put("conn_oracle_source_table_Password",context.conn_oracle_source_table_Password);
                                    } catch (java.lang.RuntimeException e) {
                                        //do nothing
                                    }
                                }
                            }
                        }
                        context.setContextType("conn_oracle_source_table_Schema", "id_String");
                        if(context.getStringValue("conn_oracle_source_table_Schema") == null) {
                            context.conn_oracle_source_table_Schema = null;
                        } else {
                            context.conn_oracle_source_table_Schema=(String) context.getProperty("conn_oracle_source_table_Schema");
                        }
                        context.setContextType("conn_oracle_source_table_Server", "id_String");
                        if(context.getStringValue("conn_oracle_source_table_Server") == null) {
                            context.conn_oracle_source_table_Server = null;
                        } else {
                            context.conn_oracle_source_table_Server=(String) context.getProperty("conn_oracle_source_table_Server");
                        }
                        context.setContextType("conn_oracle_source_table_Port", "id_String");
                        if(context.getStringValue("conn_oracle_source_table_Port") == null) {
                            context.conn_oracle_source_table_Port = null;
                        } else {
                            context.conn_oracle_source_table_Port=(String) context.getProperty("conn_oracle_source_table_Port");
                        }
                        context.setContextType("conn_oracle_source_table_ServiceName", "id_String");
                        if(context.getStringValue("conn_oracle_source_table_ServiceName") == null) {
                            context.conn_oracle_source_table_ServiceName = null;
                        } else {
                            context.conn_oracle_source_table_ServiceName=(String) context.getProperty("conn_oracle_source_table_ServiceName");
                        }
                        context.setContextType("conn_oracle_source_table_Login", "id_String");
                        if(context.getStringValue("conn_oracle_source_table_Login") == null) {
                            context.conn_oracle_source_table_Login = null;
                        } else {
                            context.conn_oracle_source_table_Login=(String) context.getProperty("conn_oracle_source_table_Login");
                        }
                        context.setContextType("conn_oracle_source_table__AdditionalParams", "id_String");
                        if(context.getStringValue("conn_oracle_source_table__AdditionalParams") == null) {
                            context.conn_oracle_source_table__AdditionalParams = null;
                        } else {
                            context.conn_oracle_source_table__AdditionalParams=(String) context.getProperty("conn_oracle_source_table__AdditionalParams");
                        }
                        context.setContextType("field_separator", "id_String");
                        if(context.getStringValue("field_separator") == null) {
                            context.field_separator = null;
                        } else {
                            context.field_separator=(String) context.getProperty("field_separator");
                        }
                        context.setContextType("header", "id_Integer");
                        if(context.getStringValue("header") == null) {
                            context.header = null;
                        } else {
                            try{
                                context.header=routines.system.ParserUtils.parseTo_Integer (context.getProperty("header"));
                            } catch(NumberFormatException e){
                                log.warn(String.format("Null value will be used for context parameter %s: %s", "header", e.getMessage()));
                                context.header=null;
                            }
                        }
                } 
                public void processAllContext() {
                        processContext_0();
                }
            }

            new ContextProcessing().processAllContext();
        } catch (java.io.IOException ie) {
            System.err.println("Could not load context "+contextStr);
            ie.printStackTrace();
        }

        // get context value from parent directly
        if (parentContextMap != null && !parentContextMap.isEmpty()) {if (parentContextMap.containsKey("conn_hive_target_table_AdditionalJDBCParameters")) {
                context.conn_hive_target_table_AdditionalJDBCParameters = (String) parentContextMap.get("conn_hive_target_table_AdditionalJDBCParameters");
            }if (parentContextMap.containsKey("conn_hive_target_table_Database")) {
                context.conn_hive_target_table_Database = (String) parentContextMap.get("conn_hive_target_table_Database");
            }if (parentContextMap.containsKey("conn_hive_target_table_Password")) {
                context.conn_hive_target_table_Password = (java.lang.String) parentContextMap.get("conn_hive_target_table_Password");
            }if (parentContextMap.containsKey("conn_hive_target_table_Server")) {
                context.conn_hive_target_table_Server = (String) parentContextMap.get("conn_hive_target_table_Server");
            }if (parentContextMap.containsKey("conn_hive_target_table_HiveMetastorePort")) {
                context.conn_hive_target_table_HiveMetastorePort = (String) parentContextMap.get("conn_hive_target_table_HiveMetastorePort");
            }if (parentContextMap.containsKey("conn_hive_target_table_Port")) {
                context.conn_hive_target_table_Port = (String) parentContextMap.get("conn_hive_target_table_Port");
            }if (parentContextMap.containsKey("conn_hive_target_table_Login")) {
                context.conn_hive_target_table_Login = (String) parentContextMap.get("conn_hive_target_table_Login");
            }if (parentContextMap.containsKey("conn_sftp_mbtc_username")) {
                context.conn_sftp_mbtc_username = (String) parentContextMap.get("conn_sftp_mbtc_username");
            }if (parentContextMap.containsKey("conn_sftp_mbtc_host")) {
                context.conn_sftp_mbtc_host = (String) parentContextMap.get("conn_sftp_mbtc_host");
            }if (parentContextMap.containsKey("conn_sftp_mbtc_port")) {
                context.conn_sftp_mbtc_port = (Integer) parentContextMap.get("conn_sftp_mbtc_port");
            }if (parentContextMap.containsKey("conn_sftp_mbtc_password")) {
                context.conn_sftp_mbtc_password = (java.lang.String) parentContextMap.get("conn_sftp_mbtc_password");
            }if (parentContextMap.containsKey("conn_hadoop_cluster_User")) {
                context.conn_hadoop_cluster_User = (String) parentContextMap.get("conn_hadoop_cluster_User");
            }if (parentContextMap.containsKey("conn_hadoop_cluster_NameNodeUri")) {
                context.conn_hadoop_cluster_NameNodeUri = (String) parentContextMap.get("conn_hadoop_cluster_NameNodeUri");
            }if (parentContextMap.containsKey("target_count")) {
                context.target_count = (Integer) parentContextMap.get("target_count");
            }if (parentContextMap.containsKey("src_file_dir")) {
                context.src_file_dir = (String) parentContextMap.get("src_file_dir");
            }if (parentContextMap.containsKey("filename")) {
                context.filename = (String) parentContextMap.get("filename");
            }if (parentContextMap.containsKey("file_extension")) {
                context.file_extension = (String) parentContextMap.get("file_extension");
            }if (parentContextMap.containsKey("hdfs_raw_src_file_dir")) {
                context.hdfs_raw_src_file_dir = (String) parentContextMap.get("hdfs_raw_src_file_dir");
            }if (parentContextMap.containsKey("source_schema")) {
                context.source_schema = (String) parentContextMap.get("source_schema");
            }if (parentContextMap.containsKey("target_schema")) {
                context.target_schema = (String) parentContextMap.get("target_schema");
            }if (parentContextMap.containsKey("target_tablename")) {
                context.target_tablename = (String) parentContextMap.get("target_tablename");
            }if (parentContextMap.containsKey("target_table_location")) {
                context.target_table_location = (String) parentContextMap.get("target_table_location");
            }if (parentContextMap.containsKey("create_table_schema")) {
                context.create_table_schema = (String) parentContextMap.get("create_table_schema");
            }if (parentContextMap.containsKey("select_columns")) {
                context.select_columns = (String) parentContextMap.get("select_columns");
            }if (parentContextMap.containsKey("source_count")) {
                context.source_count = (Integer) parentContextMap.get("source_count");
            }if (parentContextMap.containsKey("job_server_file_dir")) {
                context.job_server_file_dir = (String) parentContextMap.get("job_server_file_dir");
            }if (parentContextMap.containsKey("audit_database")) {
                context.audit_database = (String) parentContextMap.get("audit_database");
            }if (parentContextMap.containsKey("manifest_table")) {
                context.manifest_table = (String) parentContextMap.get("manifest_table");
            }if (parentContextMap.containsKey("conn_oracle_source_table_Password")) {
                context.conn_oracle_source_table_Password = (java.lang.String) parentContextMap.get("conn_oracle_source_table_Password");
            }if (parentContextMap.containsKey("conn_oracle_source_table_Schema")) {
                context.conn_oracle_source_table_Schema = (String) parentContextMap.get("conn_oracle_source_table_Schema");
            }if (parentContextMap.containsKey("conn_oracle_source_table_Server")) {
                context.conn_oracle_source_table_Server = (String) parentContextMap.get("conn_oracle_source_table_Server");
            }if (parentContextMap.containsKey("conn_oracle_source_table_Port")) {
                context.conn_oracle_source_table_Port = (String) parentContextMap.get("conn_oracle_source_table_Port");
            }if (parentContextMap.containsKey("conn_oracle_source_table_ServiceName")) {
                context.conn_oracle_source_table_ServiceName = (String) parentContextMap.get("conn_oracle_source_table_ServiceName");
            }if (parentContextMap.containsKey("conn_oracle_source_table_Login")) {
                context.conn_oracle_source_table_Login = (String) parentContextMap.get("conn_oracle_source_table_Login");
            }if (parentContextMap.containsKey("conn_oracle_source_table__AdditionalParams")) {
                context.conn_oracle_source_table__AdditionalParams = (String) parentContextMap.get("conn_oracle_source_table__AdditionalParams");
            }if (parentContextMap.containsKey("field_separator")) {
                context.field_separator = (String) parentContextMap.get("field_separator");
            }if (parentContextMap.containsKey("header")) {
                context.header = (Integer) parentContextMap.get("header");
            }
        }

        //Resume: init the resumeUtil
        resumeEntryMethodName = ResumeUtil.getResumeEntryMethodName(resuming_checkpoint_path);
        resumeUtil = new ResumeUtil(resuming_logs_dir_path, isChildJob, rootPid);
        resumeUtil.initCommonInfo(pid, rootPid, fatherPid, projectName, jobName, contextStr, jobVersion);

		List<String> parametersToEncrypt = new java.util.ArrayList<String>();
			parametersToEncrypt.add("conn_hive_target_table_Password");
			parametersToEncrypt.add("conn_sftp_mbtc_password");
			parametersToEncrypt.add("conn_oracle_source_table_Password");
        //Resume: jobStart
        resumeUtil.addLog("JOB_STARTED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "", "","","","",ResumeUtil.convertToJsonText(context,ContextProperties.class,parametersToEncrypt));

            org.slf4j.MDC.put("_context", contextStr);
            log.info("TalendJob: 'MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE' - Started.");
            java.util.Optional.ofNullable(org.slf4j.MDC.getCopyOfContextMap()).ifPresent(mdcInfo::putAll);




	
	    java.util.concurrent.ConcurrentHashMap<Object, Object> concurrentHashMap = new java.util.concurrent.ConcurrentHashMap<Object, Object>();
	    globalMap.put("concurrentHashMap", concurrentHashMap);
	

    long startUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    long endUsedMemory = 0;
    long end = 0;

    startTime = System.currentTimeMillis();


this.globalResumeTicket = true;//to run tPreJob




		if(enableLogStash) {
	        talendJobLog.addJobStartMessage();
	        try {
	            talendJobLogProcess(globalMap);
	        } catch (java.lang.Exception e) {
	            e.printStackTrace();
	        }
        }

this.globalResumeTicket = false;//to run others jobs

try {
errorCode = null;tFixedFlowInput_1Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tFixedFlowInput_1) {
globalMap.put("tFixedFlowInput_1_SUBPROCESS_STATE", -1);

e_tFixedFlowInput_1.printStackTrace();

}

this.globalResumeTicket = true;//to run tPostJob




        end = System.currentTimeMillis();

        if (watch) {
            System.out.println((end-startTime)+" milliseconds");
        }

        endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (false) {
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE");
        }
		if(enableLogStash) {
	        talendJobLog.addJobEndMessage(startTime, end, status);
	        try {
	            talendJobLogProcess(globalMap);
	        } catch (java.lang.Exception e) {
	            e.printStackTrace();
	        }
        }



    if (!inOSGi && isCBPClientPresent) {
    if(needSendForCBP && org.talend.metrics.CBPClient.getInstanceForCurrentVM() != null) {
        s("none");
        org.talend.metrics.CBPClient.getInstanceForCurrentVM().sendData();
    }
    }
    

    int returnCode = 0;


    if(errorCode == null) {
         returnCode = status != null && status.equals("failure") ? 1 : 0;
    } else {
         returnCode = errorCode.intValue();
    }
    resumeUtil.addLog("JOB_ENDED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "", "","" + returnCode,"","","");
    resumeUtil.flush();


        org.slf4j.MDC.remove("_subJobName");
        org.slf4j.MDC.remove("_subJobPid");
        org.slf4j.MDC.remove("_systemPid");
        log.info("TalendJob: 'MemoCode_job_csv_accounts_ingestion_tJavaRow_2_CODE' - Finished - status: " + status + " returnCode: " + returnCode );

    return returnCode;

  }

    // only for OSGi env
    public void destroy() {
  // add CBP code for OSGI Executions
  if (null != taskExecutionId && !taskExecutionId.isEmpty()) {
     try {
	   org.talend.metrics.DataReadTracker.setExecutionId(taskExecutionId, jobExecutionId, false);
	   org.talend.metrics.DataReadTracker.sealCounter();
	   org.talend.metrics.DataReadTracker.reset();
	} catch (Exception | NoClassDefFoundError e) {
	   // ignore
	}
  }

	// check for orphan threads if still alive after undeploy
	synchronized(threadList) {
	    java.util.Iterator<Thread> it = threadList.iterator();
	    while(it.hasNext()) {
	        Thread thread = it.next();
	        if (thread != null && thread.isAlive()) {
	            System.err.println("Initiating thread cleanup prior to bundle undeployment. This is a precautionary step to ensure no no memory leaks.");
	            System.err.println("Forcefully interrupting thread with ID = " + thread.getId() + ". This may result in expected errors due to abrupt termination. Please verify if the thread was performing critical operations.");
	            thread.interrupt();
	        }
	        if (thread == null || !thread.isAlive()) {
	            it.remove();
	        }
	    }
	}
	// end of destroy()
    }













    private java.util.Map<String, Object> getSharedConnections4REST() {
        java.util.Map<String, Object> connections = new java.util.HashMap<String, Object>();






        return connections;
    }

    private void evalParam(String arg) {
        if (arg.startsWith("--resuming_logs_dir_path")) {
            resuming_logs_dir_path = arg.substring(25);
        } else if (arg.startsWith("--resuming_checkpoint_path")) {
            resuming_checkpoint_path = arg.substring(27);
        } else if (arg.startsWith("--parent_part_launcher")) {
            parent_part_launcher = arg.substring(23);
        } else if (arg.startsWith("--watch")) {
            watch = true;
        } else if (arg.startsWith("--stat_port=")) {
            String portStatsStr = arg.substring(12);
            if (portStatsStr != null && !portStatsStr.equals("null")) {
                portStats = Integer.parseInt(portStatsStr);
            }
        } else if (arg.startsWith("--trace_port=")) {
            portTraces = Integer.parseInt(arg.substring(13));
        } else if (arg.startsWith("--client_host=")) {
            clientHost = arg.substring(14);
        } else if (arg.startsWith("--context=")) {
            contextStr = arg.substring(10);
            isDefaultContext = false;
        } else if (arg.startsWith("--father_pid=")) {
            fatherPid = arg.substring(13);
        } else if (arg.startsWith("--root_pid=")) {
            rootPid = arg.substring(11);
        } else if (arg.startsWith("--father_node=")) {
            fatherNode = arg.substring(14);
        } else if (arg.startsWith("--pid=")) {
            pid = arg.substring(6);
        } else if (arg.startsWith("--context_type")) {
            String keyValue = arg.substring(15);
			int index = -1;
            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
                if (fatherPid==null) {
                    context_param.setContextType(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
                } else { // the subjob won't escape the especial chars
                    context_param.setContextType(keyValue.substring(0, index), keyValue.substring(index + 1) );
                }

            }

		} else if (arg.startsWith("--context_param")) {
            String keyValue = arg.substring(16);
            int index = -1;
            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
                if (fatherPid==null) {
                    context_param.put(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
                } else { // the subjob won't escape the especial chars
                    context_param.put(keyValue.substring(0, index), keyValue.substring(index + 1) );
                }
            }
        } else if (arg.startsWith("--context_file")) {
        	String keyValue = arg.substring(15);
        	String filePath = new String(java.util.Base64.getDecoder().decode(keyValue));
        	java.nio.file.Path contextFile = java.nio.file.Paths.get(filePath);
            try (java.io.BufferedReader reader = java.nio.file.Files.newBufferedReader(contextFile)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    int index = -1;
                    if ( (index = line.indexOf('=')) > -1) {
							if (line.startsWith("--context_param")) {
								if ("id_Password".equals(context_param.getContextType(line.substring(16, index)))) {
									context_param.put(line.substring(16, index), routines.system.PasswordEncryptUtil.decryptPassword(
											line.substring(index + 1)));
								} else {
									context_param.put(line.substring(16, index), line.substring(index + 1));
								}
							}else {//--context_type
								context_param.setContextType(line.substring(15, index), line.substring(index + 1));
							}
                    }
                }
            } catch (java.io.IOException e) {
            	System.err.println("Could not load the context file: " + filePath);
                e.printStackTrace();
            }
        } else if (arg.startsWith("--log4jLevel=")) {
            log4jLevel = arg.substring(13);
		} else if (arg.startsWith("--audit.enabled") && arg.contains("=")) {//for trunjob call
		    final int equal = arg.indexOf('=');
			final String key = arg.substring("--".length(), equal);
			System.setProperty(key, arg.substring(equal + 1));
		}
    }
    
    private static final String NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY = "<TALEND_NULL>";

    private final String[][] escapeChars = {
        {"\\\\","\\"},{"\\n","\n"},{"\\'","\'"},{"\\r","\r"},
        {"\\f","\f"},{"\\b","\b"},{"\\t","\t"}
        };
    private String replaceEscapeChars (String keyValue) {

		if (keyValue == null || ("").equals(keyValue.trim())) {
			return keyValue;
		}

		StringBuilder result = new StringBuilder();
		int currIndex = 0;
		while (currIndex < keyValue.length()) {
			int index = -1;
			// judege if the left string includes escape chars
			for (String[] strArray : escapeChars) {
				index = keyValue.indexOf(strArray[0],currIndex);
				if (index>=0) {

					result.append(keyValue.substring(currIndex, index + strArray[0].length()).replace(strArray[0], strArray[1]));
					currIndex = index + strArray[0].length();
					break;
				}
			}
			// if the left string doesn't include escape chars, append the left into the result
			if (index < 0) {
				result.append(keyValue.substring(currIndex));
				currIndex = currIndex + keyValue.length();
			}
		}

		return result.toString();
    }

    public Integer getErrorCode() {
        return errorCode;
    }


    public String getStatus() {
        return status;
    }

    ResumeUtil resumeUtil = null;
}
/************************************************************************************************
 *     100433 characters generated by Talend Cloud Data Fabric 
 *     on the 18 May 2026 at 1:58:53 pm PST
 ************************************************************************************************/
/**
 *
 */
class MemoCode__Row_input_row {
    
/**
 * 
 */
public Integer target_count;

public String getTarget_count(){
    return null;
}
public Boolean target_countIsNullable(){
    return true;
}
public Boolean target_countIsKey(){
    return false;
}
public Integer target_countLength(){
    return null;
}
public Integer target_countPrecision(){
    return null;
}
public String target_countDefault(){
    return null;
}
public String target_countComment(){
    return "";
}
public String target_countPattern(){
    return "";
}
public String target_countOriginalDbColumnName(){
    return "";
}




    public void readData(ObjectInputStream dis) {
    }
    public void readData(org.jboss.marshalling.Unmarshaller dis) {
    }
    public void writeData(ObjectOutputStream dos) {
    }
    public void writeData(org.jboss.marshalling.Marshaller dos) {
    }
    public String toLogString(){
        return null;
    }
    /**
     * Compare keys
     */
    public int compareTo(MemoCode__Row_input_row other) {
        return 0;
    }
}


