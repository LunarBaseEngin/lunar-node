
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

public class ExecutorCenter  {
	
	LunarDBServerStandAlone l_db_ssa; 
	private HashMap<CMDEnumeration.command , ExecutorInterface> executors;

	private Logger logger= null;   
	 /*
	  * <result_uuid, query result object>
	  */
	private ConcurrentHashMap<String, FTQueryResult> result_map = new ConcurrentHashMap<String, FTQueryResult>();

	
	public ExecutorCenter(LunarDBServerStandAlone db_svr, Logger _logger) 
	{  

		executors = new HashMap<CMDEnumeration.command , ExecutorInterface>();
		
		logger = _logger;
		l_db_ssa = db_svr;
		 
		
		//registerExecutor(CMDEnumeration.command.createTable, new CreateTable()); 
		
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
