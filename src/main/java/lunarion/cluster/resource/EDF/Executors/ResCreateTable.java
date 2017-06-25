
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import lunarion.cluster.coordinator.TablePartitionMeta;
import lunarion.cluster.coordinator.TaskSendReqestToNode;
import lunarion.cluster.coordinator.server.DataTypeSupported;
import lunarion.cluster.resource.QueryEngine;
import lunarion.cluster.resource.Resource;
import lunarion.cluster.resource.ResourceDistributed;
import lunarion.cluster.resource.ResponseCollector;
import lunarion.cluster.resource.EDF.ResourceExecutorInterface;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForDriver;
import lunarion.node.remote.protocol.MessageResponseForQuery;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

public class ResCreateTable implements ResourceExecutorInterface{

	public HashMap<String, String> master_map; 
	
	 
	public ResponseCollector execute(QueryEngine db_resource , String[] params, Logger logger)
	{
		ResponseCollector rc = null;
		master_map =  db_resource.getMasters();
				
		if(db_resource.isLocalMode()) 
			return  createTable(db_resource, params, logger );
		
		return null;
      
	}
	
	/*
	 * @param is as what it is required:  
	 * {@link CreateTable }. 
	 * 
	 * @return ResponseCollector
	 */
	protected ResponseCollector createTable(QueryEngine db_resource , String[] params, Logger logger)
	{
		/*
		 * params[0]: db
		 * params[1]: table
		 */
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		Iterator<String> keys = master_map.keySet().iterator();
		while(keys.hasNext())
		{
			String partition_name = keys.next();
			int partition = ControllerConstants.parsePartitionNumber(partition_name);
			if(partition >=0 )
			{
				//LunarDBClient client = master_map.get(partition_name);
				String instance_name = master_map.get(partition_name);
				LunarDBClient client = db_resource.getClientForMaster(instance_name);
				 
				CMDEnumeration.command cmd = CMDEnumeration.command.createTable; 
	        	
				String[] new_param = new String[params.length];
				new_param[0] = params[0];
				new_param[1] = ControllerConstants.patchNameWithPartitionNumber(params[1], partition);
	        	
	        	TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, 
	        															cmd, 
	        															new_param );
	        	 
	        	Future<RemoteResult> resp = db_resource.getThreadExecutor().submit(tsqtn);
	        	responses.add(resp); 
			}
		}
		ResponseCollector rc = db_resource.patchResponseFromNodes(responses);
		
		String table_name = params[1]; 
		Resource resource_distributed = db_resource.getResource();
		
		TablePartitionMeta table_new = resource_distributed.getTablePartitionMeta(table_name);
		/*
		 * if already has, then rewrite the metadata. 
		 * if there has no existing meta file, create a new one.
		 */
		if(table_new == null)
			table_new = new TablePartitionMeta( );
		if(rc.isSucceed())
		{
			try {
				
				table_new.createMeta(resource_distributed.meta_files_path + table_name + resource_distributed.meta_file_suffix, table_name);
				resource_distributed.updateTablePartitionMeta(table_name, table_new);
				 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				 
				rc.setFalse();
				
				logger.info(Timer.currentTime() + " [EXCEPTION]: fail in creating table: " + table_name 
																+ ", the table metadata file can not be created.");
			 	
			}
		}
		else
			logger.info(Timer.currentTime() + " [ERROR]: fail to create table: " + table_name );
			
		return rc;
	}
	
}
