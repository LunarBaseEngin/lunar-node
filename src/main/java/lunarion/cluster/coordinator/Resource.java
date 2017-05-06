
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;

import io.netty.channel.ChannelFuture;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarNode;
import lunarion.node.remote.protocol.MessageResponse;
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
public class Resource {
	private AtomicInteger	NUM_NODES = new AtomicInteger(0);
	private String resource_name ;
	private int NUM_PARTITIONS = 6;
	private int NUM_REPLICAS = 2;
	private List<InstanceConfig> INSTANCE_CONFIG_LIST = new ArrayList<InstanceConfig>(); 
	
	
	private HelixAdmin admin;
	private String cluster_name;
	private ControllerConstants controller_consts = new ControllerConstants();
	
	/*
	 * <instance_name, LunarDBClient>
	 * e.g.
	 * <192.168.0.1_30001, LunarDBClient>
	 */
	private HashMap<String, LunarDBClient> instance_connection_map = new HashMap<String, LunarDBClient>(); 
	/*
	 * <partition_name, LunarDBClient>
	 */
	//private HashMap<String, InstanceConfig> master_map = new HashMap<String, InstanceConfig>(); 
	//private HashMap<String, LunarDBClient> master_map = new HashMap<String, LunarDBClient>(); 
	/*
	 * <partition_name, instance_name>
	 * e.g.
	 * <RTSeventhDB_1, 192.168.0.1_30003>
	 */
	private HashMap<String, String> master_map = new HashMap<String, String>(); 
	/*
	 * <partition_name, array_list_of_slave_instance_names>
	 * e.g.
	 * <RTSeventhDB_1, (192.168.0.1_30000, 192.168.0.1_30001>
	 */
	private HashMap<String, ArrayList<String>> slaves_map = new HashMap<String, ArrayList<String>>(); 
	
	
	private final int parallel = Runtime.getRuntime().availableProcessors() ;
	   
	protected ExecutorService thread_executor = Executors.newFixedThreadPool(parallel); 
	   
	
	public Resource(HelixAdmin _admin, String _cluster_name, String _res_name, int _num_partitions, int _num_replicas)
	{
		admin = _admin;
		cluster_name = _cluster_name;
		resource_name = _res_name;
		NUM_PARTITIONS = _num_partitions;
		NUM_REPLICAS = _num_replicas;
	}
	
	public void close()
	{
		if(thread_executor != null)
			thread_executor.shutdownNow();
	}
	
	public void addNode(String ip, int port) throws Exception
	{
		InstanceConfig instanceConfig = new InstanceConfig(ip + "_" + port);
		 instanceConfig.setHostName(ip);
		 instanceConfig.setPort("" + port);
		 instanceConfig.setInstanceEnabled(true);
		 Screen.echo("ADDING NEW remote NODE :" + instanceConfig.getInstanceName()
		        + " to resource "+ this.resource_name +". Partitions will move from old nodes to the new node.");
		 admin.addInstance(cluster_name, instanceConfig);
		 INSTANCE_CONFIG_LIST.add(instanceConfig);
		 NUM_NODES.incrementAndGet();
		 /*
		  * if it is the first node of this resource, must balance to set it to be the master.
		  */
		 //if(NUM_NODES == 1)
		// {
		 		admin.rebalance(cluster_name, resource_name, NUM_NODES.get()); 
			 
			 boolean the_node_connected = false;
			 while(!the_node_connected)
			 {
				 Thread.sleep(5000);
				 ExternalView resourceExternalView = admin.getResourceExternalView(cluster_name, resource_name);
				 TreeSet<String> sortedSet = new TreeSet<String>(resourceExternalView.getPartitionSet());
				 for (String partitionName : sortedSet) 
				 {
					 for (int i = 0; i < NUM_NODES.get(); i++) 
					 { 
						 Map<String, String> stateMap = resourceExternalView.getStateMap(partitionName);
						 if (stateMap != null && stateMap.containsKey(INSTANCE_CONFIG_LIST.get(i).getInstanceName())) 
						 {
							 the_node_connected = true;
							 Screen.echo("the node has been added for partition " +partitionName+", and balanced.");
						 } 
						 else 
							 ; 
					 }
				 }
			 }  
	}
	
 
	public void rebalance()
	{
		 admin.rebalance(cluster_name, resource_name, NUM_NODES.get());
			
	}
	
	
	public void updateMasters( ) 
	{   
		 ExternalView resourceExternalView = admin.getResourceExternalView(cluster_name, resource_name);
		 
		 TreeSet<String> sortedSet = new TreeSet<String>(resourceExternalView.getPartitionSet());
		 for (String partition_name : sortedSet) 
		 {
			 for (int i = 0; i < getNodeNumber(); i++) 
			 {   
				 String instance_name_i = getInstantConfig(i).getInstanceName();
				 
				 Map<String, String> stateMap = resourceExternalView.getStateMap(partition_name);
				 if (stateMap != null && stateMap.containsKey( instance_name_i)) 
				 {
					 if(stateMap.get(instance_name_i).equalsIgnoreCase("MASTER"))
					 { 
						 //master_map.put(getInstantConfig(i).getInstanceName(), getInstantConfig(i));
						 boolean already_has = false;
						 if(master_map.get(partition_name) != null)
						 {
							// LunarDBClient client = master_map.get(partition_name);
							 String instance_name = master_map.get(partition_name);
							 LunarDBClient client = instance_connection_map.get(instance_name);
							 
							 if(client.getConnectedHostIP().equals(getInstantConfig(i).getHostName()) 
								 && client.getConnectedPort() == Integer.parseInt(getInstantConfig(i).getPort()))
							 {
								 already_has = true;
							 } 
						 }
						 if(!already_has)
						 {
							 LunarDBClient client = new LunarDBClient();
							 try {
								client.connect(getInstantConfig(i).getHostName(), 
										LunarNode.calcDBPort(Integer.parseInt(getInstantConfig(i).getPort())));
								
								
								master_map.put(partition_name, instance_name_i); 
								instance_connection_map.put(instance_name_i, client);
							 } catch (Exception e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							 }
						 } 
					 }
					 
					 if(stateMap.get(instance_name_i).equalsIgnoreCase("SLAVE"))
					 {
						 
						 boolean already_has_this_slave = false;
						 ArrayList<String> slaves = slaves_map.get(partition_name);
						 if(slaves == null)
						 {
							 slaves = new ArrayList<String>();
							 slaves.add(instance_name_i);
							 slaves_map.put(partition_name, slaves); 
						 }
						 else
						 {
							 Iterator<String> s_itor = slaves.iterator();
							 while(s_itor.hasNext())
							 {
								 if(instance_name_i.equalsIgnoreCase(s_itor.next()))
								 {
									 already_has_this_slave = true;
									 break;
								 }
							 } 
							 if(!already_has_this_slave)
							 {
								 slaves.add(instance_name_i);
								 slaves_map.put(partition_name, slaves);
							 }
						 }
						 
					 } 
				 }
			 } 
		 }  
	}
	
	public ResponseCollector sendRequest(CMDEnumeration.command cmd, String[] params )
	{
		ResponseCollector rc = null;
		switch(cmd)
        {
        	case createTable:
        		rc = createTable(params);
        		break;
        	/*
        	case addFulltextColumn:
        		addFulltextColumn( params);
        		break;
        	case insert: 
        		insert( params);
        		break;
        	case ftQuery: 
        		ftQuery( params);  
        		break;
        		*/
        	default:
        		break;
        }
        return rc;
        		
		
	}
	
	private ResponseCollector patchResponseFromNodes(List<Future<MessageResponse>> responses)
	{
		ConcurrentHashMap<String, MessageResponse> response_map = new ConcurrentHashMap<String, MessageResponse>();
		
		for(Future<MessageResponse> resp : responses)
		{
			if(resp != null)
			{
				MessageResponse mr;
				try {
					 mr = (MessageResponse)resp.get();
					//response_map.put(mr.getUUID(), mr);
					//System.out.println(mr[0]);
					if(mr != null)
					{
						System.out.println(mr.getUUID());
						System.out.println(mr.getCMD());
						System.out.println(mr.isSucceed());
						
						response_map.put(mr.getUUID(), mr);
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
		return new ResponseCollector(response_map);
	}
	
	public ResponseCollector notifySlavesUpdate(ResponseCollector rc)
	{
		List<Future<MessageResponse>> responses = new ArrayList<Future<MessageResponse>>();
		
		CMDEnumeration.command cmd = CMDEnumeration.command.notifySlavesUpdate;
		ArrayList<String[]> list_of_tables = rc.getUpdatedTables();
		/*
		 * for notifySlavesUpdate, every element of list_of_tables is a 2-dim array: 
		 * params[0]: db name;
		 * params[1]: table name with partition id ;  
		 * 
		 */
		
		for(int i=0;i<list_of_tables.size();i++)
		{
			String[] db_and_table = list_of_tables.get(i);
			String table_i = db_and_table[1];
			int partition = controller_consts.parsePartitionNumber(table_i);
			if(partition >=0 )
			{
				String partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name,partition);
				List<String> slave_instances = slaves_map.get(partition_name);
				for(int j=0; j< slave_instances.size();j++)
				{
					LunarDBClient client = instance_connection_map.get(slave_instances.get(j)); 
					
					System.out.println("send replication message of partition " 
										+ partition_name 
										+ " to slave " 
										+ slave_instances.get(j));	
					TaskSendReqestToNode tsrtn = new TaskSendReqestToNode(partition_name, 
																		client, 
																		cmd, 
																		db_and_table );

					Future<MessageResponse> resp = thread_executor.submit(tsrtn);
					responses.add(resp); 
				}
				
			}
		}
		
		return patchResponseFromNodes(responses);
    	
	}
	
	private ResponseCollector createTable(String[] params )
	{
		ConcurrentHashMap<String, MessageResponse> response_map = new ConcurrentHashMap<String, MessageResponse>();
		List<Future<MessageResponse>> responses = new ArrayList<Future<MessageResponse>>();
		
		Iterator<String> keys = master_map.keySet().iterator();
		while(keys.hasNext())
		{
			String partition_name = keys.next();
			int partition = controller_consts.parsePartitionNumber(partition_name);
			if(partition >=0 )
			{
				//LunarDBClient client = master_map.get(partition_name);
				String instance_name = master_map.get(partition_name);
				LunarDBClient client = instance_connection_map.get(instance_name);
				 
				CMDEnumeration.command cmd = CMDEnumeration.command.createTable; 
	        	
				String[] new_param = new String[params.length];
				new_param[0] = params[0];
				new_param[1] = controller_consts.patchNameWithPartitionNumber(params[1], partition);
	        	
	        	TaskSendReqestToNode tsqtn = new TaskSendReqestToNode(partition_name, 
	        															client, 
	        															cmd, 
	        															new_param );
	        	 
	        	Future<MessageResponse> resp = thread_executor.submit(tsqtn);
	        	responses.add(resp); 
			}
		}
		
		for(Future<MessageResponse> resp : responses)
		{
			if(resp != null)
			{
				MessageResponse mr;
				try {
					 mr = (MessageResponse)resp.get();
					//response_map.put(mr.getUUID(), mr);
					//System.out.println(mr[0]);
					if(mr != null)
					{
						System.out.println(mr.getUUID());
						System.out.println(mr.getCMD());
						System.out.println(mr.isSucceed());
						
						response_map.put(mr.getUUID(), mr);
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
		return new ResponseCollector(response_map);
	}
	
	public int getNodeNumber()
	{
		return this.NUM_NODES.get();
	}
	
	public int getPartitionNumber()
	{
		return this.NUM_PARTITIONS;
	}
	
	public int getReplicaNumber()
	{
		return this.NUM_REPLICAS;
	}
	
	public InstanceConfig getInstantConfig(int i)
	{
		return INSTANCE_CONFIG_LIST.get(i);
	}
}
