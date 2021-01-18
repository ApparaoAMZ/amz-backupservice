package com.amazon.gdpr.processor;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This processor processes the Module details
 ****************************************************************************************/
@Component
public class ModuleMgmtProcessor {
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_MODULEMGMT_PROCESSOR;
		
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	/**
	 * After the completion of each module/ submodule the status of the module will be updated
	 * @param runModuleMgmt The details of the Module are passed on as input
	 * @return Boolean The status of the RunModuleMgmt table update
	 */
	public void initiateModuleMgmt(RunModuleMgmt runModuleMgmt) throws GdprException {
		String CURRENT_METHOD = "initiateModuleMgmt";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Module updates in progress.");
		RunErrorMgmt runErrorMgmt = null;
		String moduleMgmtStatus = "";
		
		try {
			runMgmtDaoImpl.insertModuleUpdates(runModuleMgmt);
			moduleMgmtStatus = GlobalConstants.MSG_MODULE_STATUS_INSERT + runModuleMgmt.getModuleName() + 
					GlobalConstants.SPACE_STRING + runModuleMgmt.getSubModuleName();
		} catch(Exception exception) {
			moduleMgmtStatus = GlobalConstants.ERR_MODULE_MGMT_INSERT + runModuleMgmt.getModuleName() + 
					GlobalConstants.SPACE_STRING + runModuleMgmt.getSubModuleName();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+moduleMgmtStatus);
			exception.printStackTrace();
			runErrorMgmt = new RunErrorMgmt(GlobalConstants.DUMMY_RUN_ID, CURRENT_CLASS, CURRENT_METHOD, 
					moduleMgmtStatus, exception.getMessage());
		}
		
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(moduleMgmtStatus);
			}
		} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + moduleMgmtStatus + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
			exception.printStackTrace();
			throw new GdprException(moduleMgmtStatus + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT);
		}
	}
	
	public String prevJobModuleStatus(long runId, String moduleName) throws GdprException {
		String CURRENT_METHOD = "prevJobModuleStatus";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Verification of Previous module status in progress.");
		RunErrorMgmt runErrorMgmt = null;		
		String moduleMgmtStatus = "";
		String prevModuleStatus = "";
		
		try {
			List<RunModuleMgmt> lstRunModuleMgmt = runMgmtDaoImpl.prevModuleRunStatus(runId, moduleName);
			int totalCount = 0;
			int successCount = 0;
			int failureCount = 0;
			for(RunModuleMgmt runModuleMgmt : lstRunModuleMgmt) {
				successCount = (runModuleMgmt.getModuleStatus().equalsIgnoreCase(GlobalConstants.STATUS_SUCCESS)) ? runModuleMgmt.getCount() : successCount;
				failureCount = (runModuleMgmt.getModuleStatus().equalsIgnoreCase(GlobalConstants.STATUS_FAILURE)) ? runModuleMgmt.getCount() : failureCount;
				totalCount = totalCount + runModuleMgmt.getCount();
			}
			
			if(moduleName.equalsIgnoreCase(GlobalConstants.MODULE_INITIALIZATION)) {
				if(successCount == 4)
					prevModuleStatus = GlobalConstants.STATUS_SUCCESS;
				else if(totalCount == 4)
					prevModuleStatus = GlobalConstants.STATUS_FAILURE;
				else
					prevModuleStatus = GlobalConstants.STATUS_INPROGRESS;
			} else if (moduleName.equalsIgnoreCase(GlobalConstants.MODULE_DATABACKUP)) {
				if(successCount == 1)
					prevModuleStatus = GlobalConstants.STATUS_SUCCESS;
				else if(totalCount == 1)
					prevModuleStatus = GlobalConstants.STATUS_FAILURE;
				else
					prevModuleStatus = GlobalConstants.STATUS_INPROGRESS;
			}else {
				if(successCount == 2)
					prevModuleStatus = GlobalConstants.STATUS_SUCCESS;
				else if(totalCount == 2)
					prevModuleStatus = GlobalConstants.STATUS_FAILURE;
				else
					prevModuleStatus = GlobalConstants.STATUS_INPROGRESS;
			}			
		} catch(Exception exception) {
			moduleMgmtStatus = GlobalConstants.ERR_MODULE_MGMT_FETCH;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+moduleMgmtStatus);
			exception.printStackTrace();
			runErrorMgmt = new RunErrorMgmt(runId, CURRENT_CLASS, CURRENT_METHOD, moduleMgmtStatus, exception.getMessage());
		}
		
		try {
			if (runErrorMgmt != null) {
				gdprOutputDaoImpl.loadErrorDetails(runErrorMgmt);
				throw new GdprException(moduleMgmtStatus);
			}
		} catch (Exception exception) {
			moduleMgmtStatus = moduleMgmtStatus  + GlobalConstants.ERR_RUN_ERROR_MGMT_INSERT;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + moduleMgmtStatus);			
			exception.printStackTrace();
			throw new GdprException(moduleMgmtStatus);
		}
		return prevModuleStatus;
	}
}