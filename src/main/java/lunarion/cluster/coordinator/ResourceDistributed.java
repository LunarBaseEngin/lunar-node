
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
import lunarion.node.page.DataPage;
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
public class ResourceDistributed extends Resource{
	 
	protected final int data_page = DataPage.data_page;
	protected final int data_page_bit_len = DataPage.data_page_bit_len;
	protected final int data_page_mask = DataPage.data_page_mask;
	
	protected final long record_capacity;
	
	public ResourceDistributed(HelixAdmin _admin, String _cluster_name, String _res_name, 
					int _num_partitions, int _num_replicas, 
					int _max_rec_per_partition,
					String _meta_files_path,
					String _model_file) throws IOException
	{
		super( _admin, _cluster_name, _res_name, _num_partitions, _num_replicas, 
					_max_rec_per_partition,
					_meta_files_path,
					_model_file); 
		
		record_capacity = _num_partitions * (long)_max_rec_per_partition;
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
				
            	int remain_in_this_page_this_partition = ((rec_count_in_current_partition.get()+this.data_page ) & this.data_page_mask)
    					-  rec_count_in_current_partition.get() ;

            	
				if(remain_recs <= remain_in_this_page_this_partition)
				{  
					//if((rec_count_in_current_partition.get() + remain_recs) <= max_recs_per_partition.get()) {
					if( total_recs.get() + remain_recs  < record_capacity)	{
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
						 remain_recs = 0;
					 }
				}
				else /* remain_recs > remain_in_this_piece_this_partition */
				{
					int need_to_insert =  remain_in_this_page_this_partition;
					if( total_recs.get() + need_to_insert < record_capacity)	{
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
					else
					{
						//TODO need to warn the client that cluster is full.
						remain_recs = 0;
					}
	        		
				}
			}  
		}
		
		return patchResponseFromNodes(responses);
	}  
	
	public ResponseCollector fetchRecords(String db, String table, long from, int count, boolean if_desc)
	{ 
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		int current_partition_in_writing = table_i_meta.getLatestPartitionNumber() ; 
		
		 
		if(current_partition_in_writing >=0 )
		{ 
			String latest_partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name, current_partition_in_writing);
			
			String instance_name = master_map.get(latest_partition_name);
			LunarDBClient client = instance_connection_map.get(instance_name);
			CMDEnumeration.command cmd_for_rec_count = CMDEnumeration.command.recsCount; 
        	
			String[] new_param = new String[2];
			new_param[0] = db;
			new_param[1] = controller_consts.patchNameWithPartitionNumber(table, current_partition_in_writing); 
			TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, cmd_for_rec_count, new_param );
			RemoteResult rr = tsqtn.call();
			int rec_count_in_partition_i = 0;
			if(rr.isSucceed())
				rec_count_in_partition_i = rr.getResultCount() ;
			 	 
			int max_level = (rec_count_in_partition_i &(this.data_page_mask)) >> this.data_page_bit_len; 
			int begin_level = max_level;
			int rec_count_in_partition_i_level_n = rec_count_in_partition_i - (rec_count_in_partition_i &(this.data_page_mask));
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
			int i_th_rec_count = Math.min(rec_count_in_partition_i_level_n, this.data_page);
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
						begin_in_which_partition = this.NUM_PARTITIONS -1;
						begin_level --;
					}
								
					i_th_rec_count =  this.data_page; 
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
			
			long i_from_in_partition = i_level*data_page + i_from ; 
			 
			int g_remains = count;
			while(g_remains > 0 && (begin_in_which_partition >= 0 && begin_level >=0))
			{
				int fetch_ith_iter = (i_th_rec_count - (int)i_from_in_data_piece) >= g_remains? g_remains : (i_th_rec_count - (int)i_from_in_data_piece); 
				CMDEnumeration.command cmd = CMDEnumeration.command.fetchRecordsDESC; 
		        	
					String[]  param_fetching = new String[4];
					param_fetching[0] = db;
					param_fetching[1] = controller_consts.patchNameWithPartitionNumber(table, begin_in_which_partition);
					param_fetching[2] = ""+i_from_in_partition;
					param_fetching[3] = ""+fetch_ith_iter;
		        	TaskSendReqestToNode request_fetching_recs = new TaskSendReqestToNode( 
		        															client, 
		        															cmd, 
		        															param_fetching );
		        	 
		        	Future<RemoteResult> resp = thread_executor.submit(request_fetching_recs);
		        	
		        	responses.add(resp); 
		        	
		        	g_remains -= fetch_ith_iter;
				 
		        	/*
					 * seek previous data piece in the previous partition for more records
					 */
		        	begin_in_which_partition --;
		        	if(begin_in_which_partition < 0)
		        	{
		        		//return patchResponseFromNodes(responses);
		        		begin_in_which_partition = this.NUM_PARTITIONS;
		        		i_level++;
		        	}
		        	//i_from = 0;
		        	//i_th_rec_count = max_recs_per_partition.get(); 
		        	i_from_in_data_piece = 0;
		        	i_from_in_partition = i_level*data_page ; 
		        	
		        	i_th_rec_count = data_page; 
			} 	
		}
	 
		
		return patchResponseFromNodes(responses);
	}
	
	
	protected ResponseCollector ftQuery(String[] params )
	{
		 
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		Iterator<String> keys = master_map.keySet().iterator();
		//int partition = controller_consts.parsePartitionNumber(latest_partition_name);
		String table = params[1];
		TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		int partition = table_i_meta.getLatestPartitionNumber() ; 
		int count = 0;
		while(count < NUM_PARTITIONS) {
			String current_partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name, partition);
			//LunarDBClient client = master_map.get(partition_name);
			
			String instance_name = master_map.get(current_partition_name);
			LunarDBClient client = instance_connection_map.get(instance_name);
			
			CMDEnumeration.command cmd = CMDEnumeration.command.ftQuery; 
        	
			String[] new_param = new String[params.length];
			new_param[0] = params[0];
			new_param[1] = controller_consts.patchNameWithPartitionNumber(params[1], partition);
        	new_param[2] = params[2];
        	new_param[3] = params[3];
        	new_param[4] = params[4];
        	TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, 
        															cmd, 
        															new_param );
        	 
        	Future<RemoteResult> resp = thread_executor.submit(tsqtn);
        	 
        	responses.add(resp); 
        	partition--;
        	if(partition < 0)
        		partition = NUM_PARTITIONS -1;
        	
        	count++;
		}
		
		return patchResponseFromNodes(responses);
	}
	
	public ResponseCollector queryRemoteWithFilter(String table, String logic_statement)
	{ 
		/*
		 * <table name, remote result>
		 */
		ConcurrentHashMap<String, RemoteResult> response_map = new ConcurrentHashMap<String, RemoteResult>();
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		Iterator<String> keys = master_map.keySet().iterator();
		
		TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		int partition = table_i_meta.getLatestPartitionNumber() ; 
		
		int count = 0;
		while(count < NUM_PARTITIONS) { 
			
			String current_partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name, partition);
			//LunarDBClient client = master_map.get(partition_name);
			
			String instance_name = master_map.get(current_partition_name);
			LunarDBClient client = instance_connection_map.get(instance_name);
			
			CMDEnumeration.command cmd = CMDEnumeration.command.filterForWhereClause; 
        	
			String[] new_param = new String[3];
			new_param[0] = this.resource_name;
			new_param[1] = controller_consts.patchNameWithPartitionNumber(table, partition);
        	new_param[2] = logic_statement; 
        	
        	TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( 
        															client, 
        															cmd, 
        															new_param );
        	 
        	Future<RemoteResult> resp = thread_executor.submit(tsqtn);
        	/*
        	RemoteResult resp_from_svr = tsqtn.call(); 
        	for(int j=0;j<resp_from_svr.getParams().length;j++)
     		{
     			System.out.println("LunarNode responded: "+ resp_from_svr.getParams()[j]);
     		}
     		*/
        	responses.add(resp); 
        	partition--;
        	 
        	if(partition < 0)
        		partition = NUM_PARTITIONS -1;
        	
        	count++;
		}
		
		return patchResponseFromNodes(responses);
	}
	
	
	protected ResponseCollector patchResponseFromNodes(List<Future<RemoteResult>> responses)
	{
		/*
		 * <i-th data piece, remote result>
		 */
		ConcurrentHashMap<Integer, RemoteResult> response_map = new ConcurrentHashMap<Integer, RemoteResult>();
		
		int i_th = 0;
		for(Future<RemoteResult> resp : responses)
		{
			if(resp != null)
			{
				RemoteResult mr;
				try {
					 mr = (RemoteResult)resp.get();
					//response_map.put(mr.getUUID(), mr);
					//System.out.println(mr[0]);
					if(mr != null)
					{
						//System.out.println(mr.getUUID());
						//System.out.println(mr.getCMD());
						//System.out.println(mr.isSucceed());
						
						//response_map.put(mr.getUUID(), mr);
						response_map.put(i_th, mr);
						i_th++;
					}
					else
					{
						System.err.println("no response");
					}
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
		}
		return new ResponseCollector (this, response_map);
	}
	
	 
}
