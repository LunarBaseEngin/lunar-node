
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
package lunarion.cluster.coordinator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.spi.SelectorProvider;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;
import org.omg.CORBA.portable.InputStream;

import LCG.FSystem.Manifold.LFSDirStore;
import LCG.FSystem.Manifold.NameFilter;
import LCG.StorageEngin.IO.L1.IOStream;
import LCG.StorageEngin.IO.L1.IOStreamNative;
import io.netty.channel.ChannelFuture;
import lunarion.cluster.coordinator.adaptor.LunarDBSchema;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command;
import lunarion.node.LunarNode;
import lunarion.node.logger.LoggerFactory;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.requester.MessageClientWatcher;
import lunarion.node.utile.ControllerConstants;
import lunarion.node.utile.Screen;

/*
 * for one resource(one db in other words in our case), if we have 3 nodes, 
 * and 6 partitions: 
			localhost_12000	localhost_12001	localhost_12002	
DataPartition_0			S			M			S		
DataPartition_1			S			S			M		
DataPartition_2			M			S			S		
DataPartition_3			S			S			M		
DataPartition_4			M			S			S		
DataPartition_5			S			M			S		
 * 
 * All the partitions together calls a resource.
 */
public class ResourceFullyDistributed extends Resource{
	 
	protected final int data_piece = 1024;// 2^10
	protected final int data_piece_mask = (~(data_piece-1));
	
	public ResourceFullyDistributed(HelixAdmin _admin, String _cluster_name, String _res_name, 
					int _num_partitions, int _num_replicas, 
					int _max_rec_per_partition,
					String _meta_files_path,
					String _model_file) throws IOException
	{
		super( _admin, _cluster_name, _res_name, _num_partitions, _num_replicas, 
					_max_rec_per_partition,
					_meta_files_path,
					_model_file); 
	}
	
 
	protected ResponseCollector insert(String[] params)
	{
		
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		 
		String db = params[0];
		String table = params[1];
		
		TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
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
				if(current_partition >= NUM_PARTITIONS)
				{
					current_partition = 0;
				}
				
				current_partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name, current_partition);
				 
				String instance_name = master_map.get(current_partition_name);
				LunarDBClient client = instance_connection_map.get(instance_name);
				
				/*
    			 * get rec count from current partition 
    			 */
    			CMDEnumeration.command cmd_getting_rec_count = CMDEnumeration.command.recsCount; 
            	String[] param_for_rec_count = new String[2];
            	param_for_rec_count[0] = db;
            	param_for_rec_count[1] = controller_consts.patchNameWithPartitionNumber(table, current_partition);
    			
            	TaskSendReqestToNode tsqtn_for_count = new TaskSendReqestToNode( 
											            			client, 
											            			cmd_getting_rec_count, 
																	param_for_rec_count );
            	Future<RemoteResult>  resp_for_count = thread_executor.submit(tsqtn_for_count);
            	try {
					rec_count_in_current_partition.set(resp_for_count.get().getResultCount());
				} catch (InterruptedException | ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					return null;
				}
				
            	int remain_in_this_piece_this_partition = ((rec_count_in_current_partition.get()+this.data_piece ) & this.data_piece_mask)
    					-  rec_count_in_current_partition.get() ;

            	
				if(remain_recs <= remain_in_this_piece_this_partition)
				{  
					if((rec_count_in_current_partition.get() + remain_recs) <= max_recs_per_partition.get()) {
		        		
		        		String[] new_param = new String[remain_recs+2];
		    			new_param[0] = params[0];
		    			new_param[1] = controller_consts.patchNameWithPartitionNumber(table, current_partition);
		    			for(int i=2;i<new_param.length;i++) {
		    				new_param[i]=params[index-2+i];
		    			}
		        		
		        		TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, 
																			   cmd, 
																			   new_param );
		        		Future<RemoteResult> resp = thread_executor.submit(tsqtn);
		        	  
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
					}
				}
				else /* remain_recs > remain_in_this_piece_this_partition */
				{
					int need_to_insert =  remain_in_this_piece_this_partition;
	    			
	    			String[] part_of_param = new String[2+need_to_insert];
	    			part_of_param[0] = params[0];
	    			part_of_param[1] = controller_consts.patchNameWithPartitionNumber(table, current_partition);
	    			for(int i=2;i<part_of_param.length;i++) {
	    				part_of_param[i]=params[index-2+i];
	    			}
	    			
	    			TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( 
																			client, 
																			cmd, 
																			part_of_param );
	    			Future<RemoteResult>  resp = thread_executor.submit(tsqtn);
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
			}  
		}
		
		return patchResponseFromNodes(responses);
	}  
	 
}
