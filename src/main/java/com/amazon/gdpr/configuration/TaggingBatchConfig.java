package com.amazon.gdpr.configuration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.amazon.gdpr.batch.TaggingJobCompletionListener;
import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.model.archive.ArchiveTable;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * This Configuration handles the Reading of SF_ARCHIVE.<Tables>,   
 * and Updating / tagging the the rows
 ****************************************************************************************/
@SuppressWarnings("unused")
@EnableScheduling
@EnableBatchProcessing
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
@Configuration
public class TaggingBatchConfig {
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_TAGGING_BATCH_CONFIG;
		
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;

	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;

	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	@Qualifier("gdprJdbcTemplate")
	JdbcTemplate jdbcTemplate;
	
	public long runId;
	public Date moduleStartDateTime = null;
	
	@Bean
	@StepScope
	public JdbcCursorItemReader<ArchiveTable> archiveTableReader(@Value("#{jobParameters[RunId]}") long runId) throws GdprException {
		String CURRENT_METHOD = "reader";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		JdbcCursorItemReader<ArchiveTable> reader = null;
		String taggingDataStatus = "";
		Boolean exceptionOccured = false;
		List<RunSummaryMgmt> lstRunSummaryMgmt = gdprOutputDaoImpl.fetchRunSummaryDetail(runId);
		
		try {
			if(lstRunSummaryMgmt != null) {
				for(RunSummaryMgmt runSummaryMgmt : lstRunSummaryMgmt) { 
					String sqlQuery = runSummaryMgmt.getTaggedQuery();
					reader = new JdbcCursorItemReader<ArchiveTable>();
					reader.setDataSource(dataSource);
					reader.setSql(sqlQuery);
					reader.setRowMapper(new ArchiveTableRowMapper(runId));
				}
			}
		} catch (Exception exception) {
			exceptionOccured = true;
			taggingDataStatus  = "Facing issues in reading Archival tables. " ;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + taggingDataStatus);
			exception.printStackTrace();		
		}
		try {
			if(exceptionOccured){
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_TAGGED, moduleStatus, moduleStartDateTime, 
						moduleStartDateTime, taggingDataStatus);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			taggingDataStatus = taggingDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+taggingDataStatus);
			throw new GdprException(taggingDataStatus); 
		}
		return reader;
	}
	
	//To set values into Tag Tables Object
	public class ArchiveTableRowMapper implements RowMapper<ArchiveTable> {
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_ARCHIVETABLEROWMAPPER;
		String tableName;
		long runId;
		
		public ArchiveTableRowMapper(long runId){
			this.runId = runId;
		}
		
		@Override
		public ArchiveTable mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";
	        return new ArchiveTable(runId, rs.getString("IMPACT_TABLE_NAME"), rs.getLong("ID"), rs.getString("COUNTRY_CODE"), 
	        		rs.getInt("CATEGORY_ID"), "SCHEDULED"); 
		}
	}
	
	public class TaggingProcessor implements ItemProcessor<ArchiveTable, ArchiveTable>{
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_JOB_TAGGINGPROCESSOR;
		//private Map<String, String> mapTableIdToName = null;
		
		@BeforeStep
		public void beforeStep(final StepExecution stepExecution) throws GdprException {
			String CURRENT_METHOD = "beforeStep";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Before Step : "+LocalTime.now());
			
			long currentRun;
			
			JobParameters jobParameters = stepExecution.getJobParameters();
			runId	= jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
			currentRun 	= jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
			
			//mapTableIdToName = gdprInputDaoImpl.fetchImpactTableMap(GlobalConstants.IMPACTTABLE_MAP_IDTONAME);			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);			
		}
		
		@Override
		public ArchiveTable process(ArchiveTable arg0) throws Exception {
			String CURRENT_METHOD = "process";		
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		
			//arg0.setTableName(mapTableIdToName.get(arg0.getTableId()));
			return arg0;
		}
	}
	
	public class TagInputWriter<T> implements ItemWriter<ArchiveTable> { 
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_TAGINPUTWRITER;		
		private final JdbcTemplate jdbcTemplate;	
		
		public TagInputWriter(JdbcTemplate jdbcTemplate) {
			this.jdbcTemplate = jdbcTemplate;
		}
					
		@Override
		public void write(List<? extends ArchiveTable> lstArchiveTable) throws GdprException {
			String CURRENT_METHOD = "write";
			Boolean exceptionOccured = false;
			String tagArchivalDataStatus = "";
			
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			try {
				for(ArchiveTable archiveTable : lstArchiveTable) {
					String sqlQuery = "INSERT INTO TAG."+archiveTable.getTableName()+"(RUN_ID, ID, CATEGORY_ID, COUNTRY_CODE, STATUS) "
							+ " VALUES (?,?,?, ?,?)";				
					jdbcTemplate.update(sqlQuery, archiveTable.getRunId(), archiveTable.getId(), archiveTable.getCategoryId(),
							archiveTable.getCountryCode(), archiveTable.getStatus());
				}
			} catch (Exception exception) {
				exceptionOccured = true;
				tagArchivalDataStatus  = "Facing issues while writing data into archival table. ";
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + tagArchivalDataStatus);
				exception.printStackTrace();
			}
			try {
				if(exceptionOccured){
					String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
					Date moduleStartDateTime = new Date();
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
							GlobalConstants.SUB_MODULE_TAGGED, moduleStatus, moduleStartDateTime, 
							moduleStartDateTime, tagArchivalDataStatus);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				}
			} catch(GdprException exception) {
				tagArchivalDataStatus = tagArchivalDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+tagArchivalDataStatus);
				throw new GdprException(tagArchivalDataStatus); 
			}
		}
	}
	
	@Bean
	public Step taggingStep() throws GdprException {
		String CURRENT_METHOD = "taggingStep";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		Step step = null;
		Boolean exceptionOccured = false;
				
		String taggingStatus = "";
		try {			
			step = stepBuilderFactory.get(CURRENT_METHOD)
				.<ArchiveTable, ArchiveTable> chunk(SqlQueriesConstant.BATCH_ROW_COUNT)
				.reader(archiveTableReader(0))
				.processor(new TaggingProcessor())
				.writer(new TagInputWriter<Object>(jdbcTemplate))
				.build();
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + GlobalConstants.ERR_TAGGING_LOAD);
			exceptionOccured = true;
			exception.printStackTrace();
			taggingStatus  = GlobalConstants.ERR_TAGGING_LOAD ;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+taggingStatus);
		}
		try {
			if(exceptionOccured){
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_TAGGED, moduleStatus, moduleStartDateTime, 
						moduleStartDateTime, taggingStatus);				
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			taggingStatus = taggingStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+taggingStatus);
			throw new GdprException(taggingStatus); 
		}
		return step;
	}
	
	
	@Bean
	public Job processTaggingJob() throws GdprException{
		String CURRENT_METHOD = "processTaggingJob";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Batch Process : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		Job job = null;
		Boolean exceptionOccured = false;
		String taggingDataStatus = "";
		
		try {
			job = jobBuilderFactory.get(CURRENT_METHOD)
					.incrementer(new RunIdIncrementer()).listener(taggingListener(GlobalConstants.JOB_TAGGING))										
					.flow(taggingStep())
					.end()
					.build();
		} catch(Exception exception) {
			exceptionOccured = true;
			taggingDataStatus = GlobalConstants.ERR_TAG_JOB_PROCESS;
			exception.printStackTrace();
		}
		try {
			if(exceptionOccured){
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				Date moduleStartDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_TAGGED, moduleStatus, moduleStartDateTime, 
						moduleStartDateTime, taggingDataStatus);	
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			taggingDataStatus = taggingDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+taggingDataStatus);
			throw new GdprException(taggingDataStatus); 
		}
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Batch Process : "+LocalTime.now());
		return job;
	}

	@Bean
	public JobExecutionListener taggingListener(String jobRelatedName) {
		String CURRENT_METHOD = "listener";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Completion listener : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		return new TaggingJobCompletionListener(jobRelatedName);
	}
}