
/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contacts: 
 * feiben@lunarion.com
 * neo.carmack@lunarion.com
 *  
 * The contents of this file are subject to the Lunarion Public License Version 1.0
 * ("License"); You may not use this file except in compliance with the License.
 * The Original Code is:  LunarBase source code 
 * The LunarBase source code is managed by the development team at Lunarion.com.
 * The Initial Developer of the Original Code is the development team at Lunarion.com.
 * Portions created by lunarion are Copyright (C) lunarion.
 * All Rights Reserved.
 *******************************************************************************
 * 
 */
package lunarion.node.EDF;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import LCG.DB.API.Result.FTQueryResult;
import LCG.EnginEvent.Event;
import LCG.EnginEvent.Interfaces.LFuture;
import LCG.EnginEvent.Interfaces.LHandler;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.executors.AddFunctionalColumn;
import lunarion.node.EDF.executors.CloseQueryResult;
import lunarion.node.EDF.executors.CreateTable;
import lunarion.node.EDF.executors.FTQuery;
import lunarion.node.EDF.executors.FetchLog;
import lunarion.node.EDF.executors.FetchQueryResultRecs;
import lunarion.node.EDF.executors.FetchRecords;
import lunarion.node.EDF.executors.FetchTableNamesWithSuffix;
import lunarion.node.EDF.executors.FilterForWhereClause;
import lunarion.node.EDF.executors.GetColumns;
import lunarion.node.EDF.executors.Insert;
import lunarion.node.EDF.executors.NotifySlavesUpdate;
import lunarion.node.EDF.executors.RGQuery;
import lunarion.node.EDF.executors.RecsCount;
import lunarion.node.remote.protocol.MessageToWrite;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public class ExecutorCenter {
	
	LunarDBServerStandAlone l_db_ssa; 
	private Logger logger= null;
	   
	 /*
	  * <result_uuid, query result object>
	  */
	private ConcurrentHashMap<String, FTQueryResult> result_map = new ConcurrentHashMap<String, FTQueryResult>();

	private HashMap<CMDEnumeration.command , ExecutorInterface> executors;

	public ExecutorCenter(LunarDBServerStandAlone db_svr, Logger _logger) 
	{  
		l_db_ssa = db_svr;
		logger = _logger;
		executors = new HashMap<CMDEnumeration.command , ExecutorInterface>();
		
		//registerExecutor(CMDEnumeration.command.createTable, new CreateTable());
		executors.put(CMDEnumeration.command.createTable,  new CreateTable());
		executors.put(CMDEnumeration.command.notifySlavesUpdate,  new NotifySlavesUpdate());
		executors.put(CMDEnumeration.command.addFulltextColumn,  new AddFunctionalColumn(CMDEnumeration.command.addFulltextColumn));
		executors.put(CMDEnumeration.command.addAnalyticColumn,  new AddFunctionalColumn(CMDEnumeration.command.addAnalyticColumn));
		executors.put(CMDEnumeration.command.addStorableColumn,  new AddFunctionalColumn(CMDEnumeration.command.addStorableColumn));
		executors.put(CMDEnumeration.command.insert,  new Insert());
		executors.put(CMDEnumeration.command.ftQuery,  new FTQuery(result_map));
		executors.put(CMDEnumeration.command.rgQuery,  new RGQuery(result_map));
		executors.put(CMDEnumeration.command.fetchQueryResultRecs,  new FetchQueryResultRecs(result_map));
		executors.put(CMDEnumeration.command.closeQueryResult,  new CloseQueryResult(result_map));
		executors.put(CMDEnumeration.command.fetchRecordsDESC,  new FetchRecords(true));
		executors.put(CMDEnumeration.command.fetchRecordsASC,  new FetchRecords(false));
		executors.put(CMDEnumeration.command.fetchLog,  new FetchLog( ));
		executors.put(CMDEnumeration.command.fetchTableNamesWithSuffix,  new FetchTableNamesWithSuffix());
		executors.put(CMDEnumeration.command.getColumns,  new GetColumns());
		executors.put(CMDEnumeration.command.filterForWhereClause,  new FilterForWhereClause(result_map));
		executors.put(CMDEnumeration.command.recsCount,  new RecsCount( ));
		
		
		
	}

	public ConcurrentHashMap<String, FTQueryResult> getResultMap()
	{
		return this.result_map;
	}
	public LunarDBServerStandAlone getActiveServer()
	{
		return l_db_ssa;
	}
	
	public Logger getLogger()
	{
		return logger;
	}
	public void registerExecutor(CMDEnumeration.command cmd, ExecutorInterface executor) { 
		 executors.put(cmd, executor);
	}
	
	public MessageResponse dispatch(MessageRequest _msg) {
		  return executors.get(_msg.getCMD()).execute(l_db_ssa, _msg, logger) ;
	}
	
	public void removeExecutor(CMDEnumeration.command cmd, ExecutorInterface executor) { 
		executors.remove(cmd, executor); 
	}
	
	public boolean hasExecutor(CMDEnumeration.command cmd)
	{
		return executors.containsKey(cmd);
	}
	
	public ExecutorInterface replaceExecutor(CMDEnumeration.command cmd, ExecutorInterface executor)
	{  
		return executors.put(cmd, executor);
	}

}
