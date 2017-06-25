
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

import lunarion.cluster.coordinator.TablePartitionMeta;
import lunarion.cluster.coordinator.TaskSendReqestToNode;
import lunarion.cluster.resource.QueryEngine;
import lunarion.cluster.resource.ResourceDistributed;
import lunarion.cluster.resource.ResponseCollector;
import lunarion.cluster.resource.EDF.ResourceExecutorInterface;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

public class ResFulltextQuery implements ResourceExecutorInterface{

	public HashMap<String, String> master_map; 
	
	 
	public ResponseCollector execute(QueryEngine db_resource , String[] params, Logger logger)
	{
		ResponseCollector rc = null;
		master_map =  db_resource.getMasters(); 
		 
   	 
		return  ftQuery(db_resource, params, logger );
      
	}

	protected ResponseCollector ftQuery(QueryEngine db_resource , String[] params, Logger logger )
	{
		 
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		Iterator<String> keys = master_map.keySet().iterator();
		//int partition = controller_consts.parsePartitionNumber(latest_partition_name);
		String db = params[0];
		String table = params[1];
		//TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		//TablePartitionMeta table_i_meta =  db_resource.getTablePartitionMeta(table); 
		//int partition = table_i_meta.getLatestPartitionNumber() ; 
		
		int partition = db_resource.currentPartitionInWriting( table);
		int count = 0;
		while(count < db_resource.NUM_PARTITIONS) {
			String current_partition_name = ControllerConstants.patchNameWithPartitionNumber(db, partition);
			//LunarDBClient client = master_map.get(partition_name);
			
			String instance_name = master_map.get(current_partition_name);
			//LunarDBClient client = instance_connection_map.get(instance_name);
			LunarDBClient client = db_resource.getClientForMaster(instance_name);
			
			CMDEnumeration.command cmd = CMDEnumeration.command.ftQuery; 
        	
			String[] new_param = new String[params.length];
			new_param[0] = params[0];
			new_param[1] = ControllerConstants.patchNameWithPartitionNumber(params[1], partition);
        	new_param[2] = params[2];
        	new_param[3] = params[3];
        	new_param[4] = params[4];
        	TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, 
        															cmd, 
        															new_param );
        	 
        	Future<RemoteResult> resp = db_resource.getThreadExecutor().submit(tsqtn);
        	 
        	responses.add(resp); 
        	partition--;
        	if(partition < 0)
        		partition = db_resource.NUM_PARTITIONS -1;
        	
        	count++;
		}
		
		return db_resource.patchResponseFromNodes(responses);
	}
	
}

