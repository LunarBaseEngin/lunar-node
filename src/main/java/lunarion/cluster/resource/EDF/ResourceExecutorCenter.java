
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
package lunarion.cluster.resource.EDF;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import LCG.DB.API.Result.FTQueryResult;
import lunarion.cluster.resource.Resource;
import lunarion.cluster.resource.ResourceDistributed;
import lunarion.cluster.resource.ResponseCollector;
import lunarion.cluster.resource.EDF.Executors.ResAddFunctionalColumn;
import lunarion.cluster.resource.EDF.Executors.ResCreateTable;
import lunarion.cluster.resource.EDF.Executors.ResFetchRecords;
import lunarion.cluster.resource.EDF.Executors.ResFulltextQuery;
import lunarion.cluster.resource.EDF.Executors.ResInsert;
import lunarion.cluster.resource.EDF.Executors.ResQueryRemoteWithFilter;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.logger.LoggerFactory;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public class ResourceExecutorCenter {
	 
	
	private HashMap<CMDEnumeration.command , ResourceExecutorInterface> executors;
	private ResourceDistributed  db_resource;
	private Logger logger= null;   
	 /*
	  * <result_uuid, query result object>
	  */
	private ConcurrentHashMap<String, FTQueryResult> result_map = new ConcurrentHashMap<String, FTQueryResult>();

	
	public ResourceExecutorCenter(ResourceDistributed db_res, Logger _logger) 
	{  

		executors = new HashMap<CMDEnumeration.command , ResourceExecutorInterface>();
		
		logger = _logger;
		db_resource = db_res;
		 
		executors.put(CMDEnumeration.command.createTable, new ResCreateTable());
		executors.put(CMDEnumeration.command.addFulltextColumn, new ResAddFunctionalColumn(CMDEnumeration.command.addFulltextColumn));
		executors.put(CMDEnumeration.command.addAnalyticColumn, new ResAddFunctionalColumn(CMDEnumeration.command.addAnalyticColumn));
		executors.put(CMDEnumeration.command.addStorableColumn, new ResAddFunctionalColumn(CMDEnumeration.command.addStorableColumn));
		executors.put(CMDEnumeration.command.insert, new ResInsert());
		executors.put(CMDEnumeration.command.fetchRecordsDESC, new ResFetchRecords(CMDEnumeration.command.fetchRecordsDESC));
		executors.put(CMDEnumeration.command.fetchRecordsASC, new ResFetchRecords(CMDEnumeration.command.fetchRecordsASC));
		executors.put(CMDEnumeration.command.ftQuery, new ResFulltextQuery( ));
		executors.put(CMDEnumeration.command.filterForWhereClause, new ResQueryRemoteWithFilter( ));
		
		//registerExecutor(CMDEnumeration.command.createTable, new CreateTable()); 
		
	}

	public ConcurrentHashMap<String, FTQueryResult> getResultMap()
	{
		return this.result_map;
	}
	public Resource getResource()
	{
		return db_resource;
	}
	
	public Logger getLogger()
	{
		return logger;
	}
	public void registerExecutor(CMDEnumeration.command cmd, ResourceExecutorInterface executor) { 
		executors.put(cmd, executor);
	}
	
	public ResponseCollector dispatch(CMDEnumeration.command cmd, String[] params) {
		return executors.get(cmd).execute(db_resource, params, logger) ;
	}

	public void removeExecutor(CMDEnumeration.command cmd, ExecutorInterface executor) { 
		executors.remove(cmd, executor); 
	}
	
	public boolean hasExecutor(CMDEnumeration.command cmd)
	{
		return executors.containsKey(cmd);
	}
	
	public ResourceExecutorInterface replaceExecutor(CMDEnumeration.command cmd, ResourceExecutorInterface executor)
	{  
		return executors.put(cmd, executor);
	}
}
