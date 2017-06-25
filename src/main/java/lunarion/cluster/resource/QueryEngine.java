
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
package lunarion.cluster.resource;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lunarion.cluster.coordinator.TaskCloseIntermediateQueryResult;
import lunarion.node.page.DataPage;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;

public class QueryEngine {
	public final int data_page = DataPage.data_page;
	public final int data_page_bit_len = DataPage.data_page_bit_len;
	public final int data_page_mask = DataPage.data_page_mask;
	
	public final int NUM_PARTITIONS ;
	public final int NUM_REPLICAS ;
	/*
	 * <latest_partition_name, instance_name>
	 * e.g.
	 * <TRSeventhDB_1, 192.168.0.1_30001>
	 */
	public HashMap<String, String> master_map; 
	/*
	 * <instance_name, LunarDBClient>
	 * e.g.
	 * <192.168.0.1_30001, LunarDBClient>
	 */
	protected HashMap<String, LunarDBClient> instance_connection_map = new HashMap<String, LunarDBClient>(); 
	
	
	boolean local_mode = true;
	Resource r_d = null;
	String coordinator_addr = null;
	int coordinator_port = 0;
	
	
	protected final int parallel = Runtime.getRuntime().availableProcessors() ;
	   
	protected ExecutorService thread_executor = Executors.newFixedThreadPool(parallel); 
	
	
	public QueryEngine(Resource rd)
	{
		r_d = rd;
		master_map = rd.getMasters();
		instance_connection_map = rd.getClientMapForMaster();
		NUM_PARTITIONS = rd.NUM_PARTITIONS;
		NUM_REPLICAS = rd.NUM_REPLICAS;
	}
	
	public QueryEngine(String coordinator_addr, int coordinator_port)
	{
		local_mode = false;
		NUM_PARTITIONS = 0;
		NUM_REPLICAS = 0;
	}
	
	public boolean isLocalMode()
	{
		return this.local_mode;
	}
	
	public Resource getResource()
	{
		return this.r_d;
	}
	public HashMap<String, String> getMasters()
	{
		return this.master_map;
	}
	
	public LunarDBClient getClientForMaster(String instance_name)
	{
		return instance_connection_map.get(instance_name);
	}
	
	public int currentPartitionInWriting(String table)
	{
		if(local_mode)
		{
			return r_d.getTablePartitionMeta(table).getLatestPartitionNumber();
		}
		else
		{
			//TODO
			return -1;
		}
	}
	
	public ResponseCollector patchResponseFromNodes(List<Future<RemoteResult>> responses)
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
						/*
						 * has to put a null, otherwise in query or fetching will cause unequal number of masters and responses, 
						 * then causes the error of calculating.
						 */
						response_map.put(i_th, mr);
						i_th++;
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
	
	public Future<MessageResponse> closeIntermediateQueryResult(RemoteResult remote_result_of_query)
	{
		TaskCloseIntermediateQueryResult tsrtn = 
				new TaskCloseIntermediateQueryResult(remote_result_of_query);
		 

		Future<MessageResponse> resp = thread_executor.submit(tsrtn);
		return resp;
	}
	
	public ExecutorService getThreadExecutor()
	{
		return thread_executor;
	}
	
	
	
	
}
