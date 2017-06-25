
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import lunarion.cluster.coordinator.TablePartitionMeta;
import lunarion.cluster.coordinator.TaskSendReqestToNode;
import lunarion.cluster.resource.QueryEngine;
import lunarion.cluster.resource.Resource;
import lunarion.cluster.resource.ResourceDistributed;
import lunarion.cluster.resource.ResponseCollector;
import lunarion.cluster.resource.EDF.ResourceExecutorInterface;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

public class ResInsert  implements ResourceExecutorInterface{

	public HashMap<String, String> master_map; 
	
	 
	public ResponseCollector execute(QueryEngine db_resource , String[] params, Logger logger)
	{
		ResponseCollector rc = null;
		master_map =  db_resource.getMasters();
				
		 
		if(db_resource.isLocalMode()) 
			return  insert(db_resource, params, logger );
		
		return null;
      
	}



	/*
	 * recs in each partition, dp(data page):
	 * |__|_dp 1024_|__|  |__|_dp 1024_|__|  |__|_dp 1024_|__| ... |__|_dp 1024_|__|
	 * |__|_dp 1024_|__|  |__|_dp 1024_|__|  |__|_dp 1024_|__| ... |__|_dp 1024_|__|
	 * ...
	 * |__|_dp 1024_|__|  |__|_dp 500 _|__|
	 * 
	 * partition_0         partition_1        partition_2      ...  partition_n
	 * 
	 *                  ------^the current partition
	 *   
	 * write from the 0 partition, each data piece has 1024 records. One is full, moves to 
	 * the next partition. If reaches the maximum partition, returns to the 0 partition and 
	 * writes to the second data piece within it.
	 */
	protected ResponseCollector insert(QueryEngine db_resource , String[] params, Logger resource_logger) 
	{
		
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		 
		String db = params[0];
		String table = params[1];
		
		//TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		
		Resource resource_distributed = db_resource.getResource();
		
		
		TablePartitionMeta table_i_meta =  resource_distributed.getTablePartitionMeta(table);
		int partition = table_i_meta.getLatestPartitionNumber() ; 
		AtomicInteger rec_count_in_current_partition = table_i_meta.getRecCountInCurrentPartition();
		AtomicLong total_recs = table_i_meta.getTotalRecs();
		
		String current_partition_name; 
		if(partition >=0 )
		{ 
			CMDEnumeration.command cmd = CMDEnumeration.command.insert; 
        	
			int remain_recs =  params.length-2;
			int index = 2;
			int current_partition = partition; 
			
			while(remain_recs > 0)  
			{
				/*
				 * if reaches the maximum partition, starts over;
				 */
				if(current_partition >= db_resource.NUM_PARTITIONS )
				{
					current_partition = 0;
				}
				
				current_partition_name = ControllerConstants.patchNameWithPartitionNumber(db, current_partition);
				 
				String instance_name = master_map.get(current_partition_name);
				//LunarDBClient client = instance_connection_map.get(instance_name);
				LunarDBClient client = db_resource.getClientForMaster(instance_name);
				
				/*
    			 * get rec count from current partition 
    			 */
    			CMDEnumeration.command cmd_getting_rec_count = CMDEnumeration.command.recsCount; 
            	String[] param_for_rec_count = new String[2];
            	param_for_rec_count[0] = db;
            	param_for_rec_count[1] = ControllerConstants.patchNameWithPartitionNumber(table, current_partition);
    			
            	TaskSendReqestToNode tsqtn_for_count = new TaskSendReqestToNode( 
											            			client, 
											            			cmd_getting_rec_count, 
																	param_for_rec_count );
            	Future<RemoteResult>  resp_for_count = db_resource.getThreadExecutor().submit(tsqtn_for_count);
            	try {
					rec_count_in_current_partition.set(resp_for_count.get().getResultCount());
				} catch (InterruptedException | ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					return null;
				}
				
            	int remain_in_this_page_this_partition = ((rec_count_in_current_partition.get()+db_resource.data_page ) & db_resource.data_page_mask)
    					-  rec_count_in_current_partition.get() ;

            	
				if(remain_recs <= remain_in_this_page_this_partition)
				{  
					//if((rec_count_in_current_partition.get() + remain_recs) <= max_recs_per_partition.get()) {
					if( total_recs.get() + remain_recs  < resource_distributed.record_capacity)	{
		        		String[] new_param = new String[remain_recs+2];
		    			new_param[0] = params[0];
		    			new_param[1] = ControllerConstants.patchNameWithPartitionNumber(table, current_partition);
		    			for(int i=2;i<new_param.length;i++) {
		    				new_param[i]=params[index-2+i];
		    			}
		        		
		        		TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, 
																			   cmd, 
																			   new_param );
		        		Future<RemoteResult> resp = db_resource.getThreadExecutor().submit(tsqtn);
		        	  
		        		rec_count_in_current_partition.set(rec_count_in_current_partition.get() + remain_recs);  
		        		index += remain_recs;
		        		
		        		total_recs.addAndGet( remain_recs );
	        			
		        		remain_recs = 0; 
		        		responses.add(resp); 
		        		
		        		try { 
		        			table_i_meta.updateMeta(current_partition, rec_count_in_current_partition, total_recs.get());  
			    		}catch(Exception e) {
			    			System.out.println(e);
			    			resource_logger.info(Timer.currentTime() + " [EXCEPTION]: fail to update partition " + table_i_meta.getLatestPartition() + " metadata");
			    		 	
			    		}
					 }
					 else
					 {
						/*
						 * in this fully distributed insert, if one partition has its data full, the cluster may have the 
						 * full of data. Then here need to warn the client.
						 * 
						 * 
						 */
						//TODO 
						 remain_recs = 0;
					 }
				}
				else /* remain_recs > remain_in_this_piece_this_partition */
				{
					int need_to_insert =  remain_in_this_page_this_partition;
					if( total_recs.get() + need_to_insert < resource_distributed.record_capacity)	{
		    			String[] part_of_param = new String[2+need_to_insert];
		    			part_of_param[0] = params[0];
		    			part_of_param[1] = ControllerConstants.patchNameWithPartitionNumber(table, current_partition);
		    			for(int i=2;i<part_of_param.length;i++) {
		    				part_of_param[i]=params[index-2+i];
		    			}
		    			
		    			TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( 
																				client, 
																				cmd, 
																				part_of_param );
		    			Future<RemoteResult>  resp = db_resource.getThreadExecutor().submit(tsqtn);
		    			responses.add(resp);
		                 
		    			
		    			rec_count_in_current_partition.addAndGet(need_to_insert);
		    			total_recs.addAndGet(need_to_insert); 
		    			 
		    			//remain_in_this_piece_this_partition = this.data_piece;
		    			try { 
		        			table_i_meta.updateMeta(current_partition, rec_count_in_current_partition, total_recs.get());  
		        			 
		        		}catch(Exception e) {
		        			System.out.println(e);
		        			resource_logger.info(Timer.currentTime() + " [EXCEPTION]: fail to update partition " + table_i_meta.getLatestPartition() + " metadata");
			    		 	
		        		}
		    			
		    			remain_recs -= need_to_insert;
		    			index += need_to_insert;  
		    			current_partition++;
					}
					else
					{
						//TODO need to warn the client that cluster is full.
						remain_recs = 0;
					}
	        		
				}
			}  
		}
		
		return db_resource.patchResponseFromNodes(responses);
	}  
	
}
