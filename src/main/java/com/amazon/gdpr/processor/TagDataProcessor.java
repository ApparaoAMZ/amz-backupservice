package com.amazon.gdpr.processor;

import java.util.Date;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Service will initiate the TaggingBatchConfig Job  
 * This will be invoked by the 
 ****************************************************************************************/
@Component
public class TagDataProcessor {
	public static String CURRENT_CLASS	= GlobalConstants.CLS_TAGDATAPROCESSOR;
	
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
    Job processTaggingJob;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	Date moduleStartDateTime = null;
	
	public void taggingInitialize(long runId) throws GdprException {
		String CURRENT_METHOD = "taggingInitialize";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
				
		TaggedJobThread jobThread = new TaggedJobThread(runId);
		jobThread.start();
	}
	
	class TaggedJobThread extends Thread {
		
		long runId;
		TaggedJobThread(long runId){
			this.runId = runId;
		}
		
		@Override
		public void run() {
			String CURRENT_METHOD = "run";
			String taggingDataStatus = "";
			Boolean exceptionOccured = false;			
			Date moduleEndDateTime = null;			

			try {
				moduleStartDateTime = new Date();
				JobParametersBuilder jobParameterBuilder= new JobParametersBuilder();
				jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_ID, runId);
				jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_JOB_ID, new Date().getTime());
				
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: JobParameters set ");
				JobParameters jobParameters = jobParameterBuilder.toJobParameters();

				jobLauncher.run(processTaggingJob, jobParameters);
				taggingDataStatus = GlobalConstants.MSG_TAGGING_JOB;
			} catch (JobExecutionAlreadyRunningException | JobRestartException
					| JobInstanceAlreadyCompleteException | JobParametersInvalidException exception) {
				exceptionOccured = true;
				taggingDataStatus = GlobalConstants.ERR_TAGGED_JOB_RUN;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+taggingDataStatus);
				exception.printStackTrace();
			} 
	    	try {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				moduleEndDateTime = new Date();
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_TAGGED, moduleStatus, moduleStartDateTime, 
						moduleEndDateTime, taggingDataStatus);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			} catch(GdprException exception) {
				exceptionOccured = true;
				taggingDataStatus = taggingDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+taggingDataStatus);
			}
		}
	}
}