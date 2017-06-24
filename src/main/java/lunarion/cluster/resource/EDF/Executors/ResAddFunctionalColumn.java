
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
package lunarion.cluster.resource.EDF.Executors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import lunarion.cluster.coordinator.TaskSendReqestToNode;
import lunarion.cluster.resource.Resource;
import lunarion.cluster.resource.ResourceDistributed;
import lunarion.cluster.resource.ResponseCollector;
import lunarion.cluster.resource.EDF.ResourceExecutorInterface;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

public class ResAddFunctionalColumn implements ResourceExecutorInterface{

	public HashMap<String, String> master_map; 
	
	command cmd;
	
	public ResAddFunctionalColumn(command _cmd)
	{
		this.cmd = _cmd;
	}
	
	public ResponseCollector execute(ResourceDistributed db_resource , String[] params, Logger logger)
	{
		ResponseCollector rc = null;
		master_map =  db_resource.getMasters(); 
		 
   	 
		return  addFunctionalColumn(db_resource, params, logger );
      
	}
	/*
	 * @TaskHandlingMessage.addFunctionalColumn
	 */
	protected ResponseCollector addFunctionalColumn(ResourceDistributed db_resource, String[] params, Logger resource_logger )
	{
		
		if(cmd == command.addAnalyticColumn && params.length < 4)
    	{
    		System.err.println("[NODE ERROR]: addAnalyticColumn needs at least 4 parameters: db name, table name, column name and column type");
    		resource_logger.info("[NODE ERROR]: addAnalyticColumn needs at least 4 parameters: db name, table name, column name and column type");
			 	 
			return null;
    	}
		if(params.length < 3 )
		{
			System.err.println("[NODE ERROR]: addFulltextColumn needs at least 3 parameters: db name, table name and column name");
			resource_logger.info("[NODE ERROR]: addFulltextColumn needs at least 3 parameters: db name, table name and column name");
			 
			return null;
		}

		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		Iterator<String> keys = master_map.keySet().iterator();
		while(keys.hasNext())
		{
			String partition_name = keys.next();
			int partition = ControllerConstants.parsePartitionNumber(partition_name);
			if(partition >=0 )
			{  
				String instance_name = master_map.get(partition_name);
				//LunarDBClient client = instance_connection_map.get(instance_name);
				LunarDBClient client = db_resource.getClientForMaster(instance_name);
				
				String[] new_param = null;
				switch(cmd)
				{
					case addFulltextColumn:
					case addStorableColumn:
					{
						new_param = new String[params.length];
						new_param[0] = params[0];
						new_param[1] = ControllerConstants.patchNameWithPartitionNumber(params[1], partition);
						new_param[2] = params[2];
					}
					break;
					case addAnalyticColumn:
					{
						new_param = new String[params.length];
						new_param[0] = params[0];
						new_param[1] = ControllerConstants.patchNameWithPartitionNumber(params[1], partition);
						new_param[2] = params[2];
						new_param[3] = params[3];
					}
					break; 
				}
	        	TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client,cmd,new_param );
	        	 
	        	Future<RemoteResult> resp = db_resource.getThreadExecutor().submit(tsqtn);
	        	responses.add(resp); 
			}
		}
		
		return db_resource.patchResponseFromNodes(responses);
	}
	
}

