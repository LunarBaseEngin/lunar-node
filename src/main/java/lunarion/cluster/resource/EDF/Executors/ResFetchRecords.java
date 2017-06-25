
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
import lunarion.db.local.shell.CMDEnumeration.command;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

public class ResFetchRecords implements ResourceExecutorInterface{

	public HashMap<String, String> master_map; 
	
	command cmd;
	
	public ResFetchRecords(command _cmd)
	{
		this.cmd = _cmd;
	} 
	public ResponseCollector execute(QueryEngine db_resource , String[] params, Logger logger)
	{
		ResponseCollector rc = null;
		master_map =  db_resource.getMasters(); 
		 
   	 
		String db = params[0]; 
		String table = params[1]; 
		long from = Long.parseLong(params[2].trim());
		int count = Integer.parseInt(params[3].trim());
		boolean if_desc = true;
		 
		if(this.cmd == CMDEnumeration.command.fetchRecordsASC)
			if_desc = false;
		
		return  fetchRecords(db_resource, db, table , from, count, if_desc, logger);
	}
	
	
	public ResponseCollector fetchRecords(QueryEngine db_resource, String db, String table, long from, int count, boolean if_desc, Logger logger)
	{ 
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		//TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		//TablePartitionMeta table_i_meta =  db_resource.getTablePartitionMeta(table);
		
		//int current_partition_in_writing = table_i_meta.getLatestPartitionNumber() ; 
		int current_partition_in_writing = db_resource.currentPartitionInWriting(table);
		 
		if(current_partition_in_writing >=0 )
		{ 
			String latest_partition_name = ControllerConstants.patchNameWithPartitionNumber(db, current_partition_in_writing);
			
			String instance_name = master_map.get(latest_partition_name);
			//LunarDBClient client = instance_connection_map.get(instance_name);
			LunarDBClient client = db_resource.getClientForMaster(instance_name);
			
			
			CMDEnumeration.command cmd_for_rec_count = CMDEnumeration.command.recsCount; 
        	
			String[] new_param = new String[2];
			new_param[0] = db;
			new_param[1] = ControllerConstants.patchNameWithPartitionNumber(table, current_partition_in_writing); 
			TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, cmd_for_rec_count, new_param );
			RemoteResult rr = tsqtn.call();
			int rec_count_in_partition_i = 0;
			if(rr.isSucceed())
				rec_count_in_partition_i = rr.getResultCount() ;
			 	 
			int max_level = (rec_count_in_partition_i &(db_resource.data_page_mask)) >> db_resource.data_page_bit_len; 
			int begin_level = max_level;
			int rec_count_in_partition_i_level_n = rec_count_in_partition_i - (rec_count_in_partition_i &(db_resource.data_page_mask));
			/*
			 * recs in each partition, dp(data page):
			 * level 0: |__|_dp 1024_|__|  |__|_dp 1024_|__|  |__|_dp 1024_|__| ... |__|_dp 1024_|__|
			 * level 1: |__|_dp 1024_|__|  |__|_dp 1024_|__|  |__|_dp 1024_|__| ... |__|_dp 1024_|__|
			 * ...
			 * level n: |__|_dp 1024_|__|  |__|_dp 500 _|__|
			 * 
			 *             partition_0         partition_1        partition_2      ...  partition_n
			 * 
			 *                             			^------the current partition
			 * if fetch recs from position X, seek in which data piece, belonging to which partition, the position X is.  
			 *                   
			 */
			int begin_in_which_partition =  current_partition_in_writing;
			int i_th_rec_count = Math.min(rec_count_in_partition_i_level_n, db_resource.data_page);
			long i_from = from;
			/*
			 * find out in which partition to begin fetching records
			 */
			while( i_from >= 0 )
			{
				//i_from = (i_from - i_th_rec_count )>0? ( i_from - i_th_rec_count ) : i_from;
				i_from = (i_from - i_th_rec_count );
				if(i_from >= 0)
				{
					/*
					 * move to the previous partition, and check its latest data piece.
					 */
					begin_in_which_partition --;
					if(begin_in_which_partition < 0)
					{
						begin_in_which_partition = db_resource.NUM_PARTITIONS -1;
						begin_level --;
					}
								
					i_th_rec_count =  db_resource.data_page; 
				} 
			} 
			
			i_from += i_th_rec_count;
			int i_level = 0;
			
			if(begin_in_which_partition  <= current_partition_in_writing)
			{
				i_level = max_level - begin_level;
			}
			else
			{
				i_level = max_level -1 - begin_level;
			}
			
			long i_from_in_data_piece = i_from;
			
			long i_from_in_partition = i_level*db_resource.data_page + i_from ; 
			 
			int g_remains = count;
			while(g_remains > 0 && (begin_in_which_partition >= 0 && begin_level >=0))
			{
				int fetch_ith_iter = (i_th_rec_count - (int)i_from_in_data_piece) >= g_remains? g_remains : (i_th_rec_count - (int)i_from_in_data_piece); 
				CMDEnumeration.command cmd = CMDEnumeration.command.fetchRecordsDESC; 
		        	
					String[]  param_fetching = new String[4];
					param_fetching[0] = db;
					param_fetching[1] = ControllerConstants.patchNameWithPartitionNumber(table, begin_in_which_partition);
					param_fetching[2] = ""+i_from_in_partition;
					param_fetching[3] = ""+fetch_ith_iter;
		        	TaskSendReqestToNode request_fetching_recs = new TaskSendReqestToNode( 
		        															client, 
		        															cmd, 
		        															param_fetching );
		        	 
		        	Future<RemoteResult> resp = db_resource.getThreadExecutor().submit(request_fetching_recs);
		        	
		        	responses.add(resp); 
		        	
		        	g_remains -= fetch_ith_iter;
				 
		        	/*
					 * seek previous data piece in the previous partition for more records
					 */
		        	begin_in_which_partition --;
		        	if(begin_in_which_partition < 0)
		        	{
		        		//return patchResponseFromNodes(responses);
		        		begin_in_which_partition = db_resource.NUM_PARTITIONS;
		        		i_level++;
		        	}
		        	//i_from = 0;
		        	//i_th_rec_count = max_recs_per_partition.get(); 
		        	i_from_in_data_piece = 0;
		        	i_from_in_partition = i_level*db_resource.data_page ; 
		        	
		        	i_th_rec_count = db_resource.data_page; 
			} 	
		}
	 
		
		return db_resource.patchResponseFromNodes(responses);
	}
	
	
}
