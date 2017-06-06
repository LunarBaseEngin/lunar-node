
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
public class Resource {
	private AtomicInteger	NUM_NODES = new AtomicInteger(0);
	private String resource_name ;
	private int NUM_PARTITIONS = 6;
	private int NUM_REPLICAS = 2;
	private List<InstanceConfig> INSTANCE_CONFIG_LIST = new ArrayList<InstanceConfig>(); 
	
	private String meta_files_path;
	private Logger resource_logger = null; 
	private String meta_file_suffix = ".meta.info";
	
	private HelixAdmin admin;
	private String cluster_name;
	private ControllerConstants controller_consts = new ControllerConstants();
	
	Connection sql_engine_connection = null;;
	
	AtomicInteger max_recs_per_partition = new AtomicInteger(0);/* maximum records allowed in one partition*/
	
	/* table name, metadata for latest info */
	HashMap<String, TablePartitionMeta> table_meta_map = new HashMap<String, TablePartitionMeta>(); 
	
	 
	//String latest_partition_name = null;/* the maximum partition currently*/
	//AtomicInteger rec_count_in_current_partition= new AtomicInteger(0);/* the record count in the latest partition*/
	
	//Properties prop;
	//FileOutputStream oFile  ;
		
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
	
	
	//Iterator<String> tables_in_resource;
	
	private final int parallel = Runtime.getRuntime().availableProcessors() ;
	   
	protected ExecutorService thread_executor = Executors.newFixedThreadPool(parallel); 
	   
	
	public Resource(HelixAdmin _admin, String _cluster_name, String _res_name, 
					int _num_partitions, int _num_replicas, 
					int _max_rec_per_partition,
					String _meta_files_path,
					String _model_file) throws IOException
	{
		admin = _admin;
		cluster_name = _cluster_name;
		resource_name = _res_name;
		NUM_PARTITIONS = _num_partitions;
		NUM_REPLICAS = _num_replicas;
		max_recs_per_partition.set(_max_rec_per_partition);
		resource_logger = LoggerFactory.getLogger(_cluster_name+"_"+resource_name);  
		
		resource_logger.info(Timer.currentTime() + " [INFO]: coordinator for database " + resource_name + " in the cluster: " +  _cluster_name + " is starting now. ");
	 	
		meta_files_path = _meta_files_path;
		
		File dir = new File(meta_files_path);
    	if(!dir.isDirectory())
    	{
    		resource_logger.info(Timer.currentTime() 
    							+ "[COORDINATOR EXCEPTION]:the root directory " + meta_files_path + " for global metadata is wrong, initate with a correct directory" );
    	 	
    		throw new IOException("[COORDINATOR EXCEPTION]:the root directory " + meta_files_path + " for global metadata is wrong, initate with a correct directory");
    	}
    	else
    	{
    		//NameFilter _filter = new NameFilter(meta_file_suffix);
			//File[] file_array=dir.listFiles(_filter);
    		File[] file_array=dir.listFiles( );
            if(file_array!=null)
            {
            	for (int i = 0; i < file_array.length; i++) 
            	{ 
            		String name = file_array[i].getName();
            		if(name.endsWith(meta_file_suffix))
            		{
            			String table_name = file_array[i].getName().split(meta_file_suffix)[0];
                		TablePartitionMeta table_i = new TablePartitionMeta();
                		table_i.loadMeta(meta_files_path + file_array[i].getName() );
                		table_meta_map.put(table_name, table_i);
            		}
            		
                }
            }
    	}
    	
    	//tables_in_resource = listTables();
		 
		try {
			Class.forName("org.apache.calcite.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		 
		Properties info = new Properties(); 
		
		try 
		{				
			sql_engine_connection = 
						DriverManager.getConnection("jdbc:calcite:model=" + _model_file, info);
			CalciteConnection calciteConn = sql_engine_connection.unwrap(CalciteConnection.class);

		} catch (SQLException e) {  
				e.printStackTrace(); 
				sql_engine_connection = null;
				return;
		}

		resource_logger.info(Timer.currentTime() + " [SUCCEED]: coordinator for database " + resource_name + " in the cluster: " +  _cluster_name + " started successfully. ");
	 	
	}
	
	public void close()
	{
		if(thread_executor != null)
			thread_executor.shutdownNow();
		
		 
		Iterator<String> tables = table_meta_map.keySet().iterator();
		while(tables.hasNext())
		{
			table_meta_map.get(tables.next()).close(); 
		}
		 
		
		 
        try {
        	sql_engine_connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			 
		}
	}
	
	public Future<MessageResponse> closeIntermediateQueryResult(RemoteResult remote_result_of_query)
	{
		TaskCloseIntermediateQueryResult tsrtn = 
				new TaskCloseIntermediateQueryResult(remote_result_of_query);
		 

		Future<MessageResponse> resp = thread_executor.submit(tsrtn);
		return resp;
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
							 LunarDBClient client = new LunarDBClient(true);
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
	
	public ResultSet executeSqlSelect(String sql_statement)
	{
		ResultSet result = null;
		Statement st;
		try {
			st = this.sql_engine_connection.createStatement( );
			result = st.executeQuery(sql_statement); 
			 
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	public ResponseCollector sendRequest(CMDEnumeration.command cmd, String[] params )
	{
		ResponseCollector rc = null;
		switch(cmd)
        {
	        case createTable:
	    		rc = createTable(params);
	    		break;
	    	case addFulltextColumn:
	    		rc = addFunctionalColumn(cmd, params);
	    		break;
	    	case addAnalyticColumn:
	    		rc = addFunctionalColumn(cmd, params);
	    		break;
	    	case addStorableColumn:
	    		rc = addFunctionalColumn(cmd, params);
	    		break;
	    	case insert: 
	    		rc = insert(params);
	    		break;
	    	case fetchRecordsDESC: 
	    		rc = fetchRecords( params, true);  
        		break;
        	case fetchRecordsASC: 
        		rc = fetchRecords( params, false);  
	    	case ftQuery: 
	    		rc = ftQuery(params);  
	    		break;
	    	case fetchLog:
	    		rc = fetchLog(params);
	    		break; 
	    	case sqlSelect:
	    		/*
	    		 * for sql select, params is: 
	    		 * params[0]: db name;
	    		 * 
	    		 * params[1]: select statement, e.g. select a, b, c from table_name where a<100 and c like 'what the fuck'
	    		 */
	    		rc = sqlSelect(params[1]);
	    		break;
	    	default:
	        		break;
        }
        return rc;
        		
		
	}
	
	private ResponseCollector patchResponseFromNodes(List<Future<RemoteResult>> responses)
	{
		/*
		 * <table name, remote result>
		 */
		ConcurrentHashMap<String, RemoteResult> response_map = new ConcurrentHashMap<String, RemoteResult>();
		
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
						response_map.put(mr.getTableName(), mr);
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
		return new ResponseCollector(this, response_map);
	}
	
	public ResponseCollector notifySlavesUpdate(ResponseCollector rc)
	{
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		CMDEnumeration.command cmd = CMDEnumeration.command.notifySlavesUpdate;
		ArrayList<String[]> list_of_tables = rc.getUpdatedTables();
		if(list_of_tables == null)
			return null;
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
				
				if(slave_instances != null)
				{
					for(int j=0; j< slave_instances.size();j++)
					{
						LunarDBClient client = instance_connection_map.get(slave_instances.get(j)); 
						
						System.out.println("send replication message of partition " 
											+ partition_name 
											+ " to slave " 
											+ slave_instances.get(j));	
						TaskSendReqestToNode tsrtn = new TaskSendReqestToNode( 
																			client, 
																			cmd, 
																			db_and_table );
	
						Future<RemoteResult> resp = thread_executor.submit(tsrtn);
						responses.add(resp); 
					}
				} 
			}
		}
		
		return patchResponseFromNodes(responses);
    	
	}
	
	/*
	 * @param is as what it is required:  
	 * {@link TaskHandlingMessage#createTable(String[] params) createTable}. 
	 * 
	 * @return ResponseCollector
	 */
	private ResponseCollector createTable(String[] params )
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
	        	
	        	TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, 
	        															cmd, 
	        															new_param );
	        	 
	        	Future<RemoteResult> resp = thread_executor.submit(tsqtn);
	        	responses.add(resp); 
			}
		}
		ResponseCollector rc = patchResponseFromNodes(responses);
		String table_name = params[1]; 
		TablePartitionMeta table_new = table_meta_map.get(table_name);
		/*
		 * if already has, then rewrite the metadata. 
		 * if there has no existing meta file, create a new one.
		 */
		if(table_new == null)
			table_new = new TablePartitionMeta( );
		if(rc.isSucceed())
		{
			try {
				
				table_new.createMeta(meta_files_path + table_name+meta_file_suffix, table_name);
				table_meta_map.put(table_name, table_new);
				 
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				 
				rc.setFalse();
				
				resource_logger.info(Timer.currentTime() + " [EXCEPTION]: fail in creating table: " + table_name 
																+ ", the table metadata file can not be created.");
			 	
			}
		}
		else
			resource_logger.info(Timer.currentTime() + " [ERROR]: fail to create table: " + table_name );
			
		return rc;
	}
	
	/*
	public Iterator<String> getTables()
	{
		return this.tables_in_resource;
	}
	*/
	
	public Iterator<String> listTables()
	{ 
		Iterator<String> keys = master_map.keySet().iterator();
		int seek_in_which_partition = 0;
		while(keys.hasNext())
		{
			String partition_name = keys.next();
			int partition = controller_consts.parsePartitionNumber(partition_name);
			/*
			 * seek partition 0 to find out all the tables.
			 * since as of now, all the tables must be created on every partition, 
			 * with a number suffix.
			 */
			if(partition == seek_in_which_partition )
			{
				//LunarDBClient client = master_map.get(partition_name);
				String instance_name = master_map.get(partition_name);
				LunarDBClient client = instance_connection_map.get(instance_name);
				 
				CMDEnumeration.command cmd = CMDEnumeration.command.fetchTableNamesWithSuffix;
	        	String[] params = new String[2];
	        	params[0] = this.resource_name; 
	        	params[1] = "_"+seek_in_which_partition;  
	        	
	        	RemoteResult resp_from_svr  = null;
	        	int tried = 0;
	        	try
	        	{
	        		while(resp_from_svr == null && tried <5 )
	        		{
	        			resp_from_svr = client.sendRequest(cmd, params);
	        			tried ++;
	        		}
	        	} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				 	e.printStackTrace();
					
				 	return null;
				 }  
	        	if(resp_from_svr == null)
	        	{
	        		resource_logger.info(Timer.currentTime() 
							+ "[COORDINATOR EXCEPTION]: @Resource.listTables can not get tables from database " + this.resource_name  );
	        		return null;
	 	
	        	}
	        	String[] arr = resp_from_svr.getParams();
	        	List<String> tables = new ArrayList<String>();
	        	for(int j=0;j<arr.length;j++)
	        	{ 
	        		tables.add(controller_consts.parseTableName(arr[j]));
	        	}
	        	return tables.iterator(); 
			}
		} 
		
		return null;
		 
	}
	
	 
	public String[] getTableColumns(String table_name)
	{ 
		Iterator<String> keys = master_map.keySet().iterator();
		int seek_in_which_partition = 0;
		while(keys.hasNext())
		{
			String partition_name = keys.next();
			int partition = controller_consts.parsePartitionNumber(partition_name);
			/*
			 * seek partition 0 to find out all the tables.
			 * since as of now, all the tables must be created on every partition, 
			 * with a number suffix.
			 */
			if(partition == seek_in_which_partition )
			{
				//LunarDBClient client = master_map.get(partition_name);
				String instance_name = master_map.get(partition_name);
				LunarDBClient client = instance_connection_map.get(instance_name);
				 
				CMDEnumeration.command cmd = CMDEnumeration.command.getColumns; 
	        	String[] params = new String[2];
	        	params[0] = this.resource_name; 
	        	params[1] =  controller_consts.patchNameWithPartitionNumber(table_name, partition);  
	        	
	        	RemoteResult resp_from_svr  = null;
	        	try
	        	{
	        		resp_from_svr = client.sendRequest(cmd, params); 
	        	} catch (InterruptedException e) {
					// TODO Auto-generated catch block
				 	e.printStackTrace(); 
				 	return null; 
	        	} 
	        	return resp_from_svr.getParams(); 
			}
		} 
		
		return null;
		 
	} 
	
	private ResponseCollector insert(String[] params)
	{
		/*
		 * <table name, remote result>
		 */
		ConcurrentHashMap<String, RemoteResult> response_map = new ConcurrentHashMap<String, RemoteResult>();
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		String table = params[1];
		
		TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		int partition = table_i_meta.getLatestPartitionNumber() ; 
		AtomicInteger rec_count_in_current_partition = table_i_meta.getRecCountInCurrentPartition();
		String latest_partition_name; 
		if(partition >=0 )
		{ 
			CMDEnumeration.command cmd = CMDEnumeration.command.insert; 
        	
			int remain_recs =  params.length-2;
			int index = 2;
			int current_partition = partition;
			while(remain_recs >0)
			{
				if(current_partition >= NUM_PARTITIONS)
				{
					break;
				}
				latest_partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name, current_partition);
				 
				String instance_name = master_map.get(latest_partition_name);
				LunarDBClient client = instance_connection_map.get(instance_name);
				
				if((rec_count_in_current_partition.get() + remain_recs) <= max_recs_per_partition.get()) {
	        		
	        		String[] new_param = new String[remain_recs+2];
	    			new_param[0] = params[0];
	    			new_param[1] = controller_consts.patchNameWithPartitionNumber(params[1], current_partition);
	    			for(int i=2;i<new_param.length;i++) {
	    				new_param[i]=params[index-2+i];
	    			}
	        		
	        		TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( 
																			client, 
																			cmd, 
																			new_param );
	        		Future<RemoteResult> resp = thread_executor.submit(tsqtn);
	        	  
	        		rec_count_in_current_partition.set(rec_count_in_current_partition.get() + remain_recs);  
	        		index += remain_recs;
	        		remain_recs = 0;
	        		responses.add(resp); 
	        		
	        		try { 
	        			table_i_meta.updateMeta(current_partition, rec_count_in_current_partition);  
		    		}catch(Exception e) {
		    			System.out.println(e);
		    			resource_logger.info(Timer.currentTime() + " [EXCEPTION]: fail to update partition " + table_i_meta.getLatestPartition() + " metadata");
		    		 	
		    		}
	    		}
				else
				{
					int need_to_insert = max_recs_per_partition.get() - rec_count_in_current_partition.get();
	    			
	    			String[] part_of_param = new String[2+need_to_insert];
	    			part_of_param[0] = params[0];
	    			part_of_param[1] = controller_consts.patchNameWithPartitionNumber(params[1], current_partition);
	    			for(int i=2;i<part_of_param.length;i++) {
	    				part_of_param[i]=params[index-2+i];
	    			}
	    			
	    			TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( 
																			client, 
																			cmd, 
																			part_of_param );
	    			Future<RemoteResult>  resp = thread_executor.submit(tsqtn);
	    			responses.add(resp);
	                 
	    			rec_count_in_current_partition.set(0);
	                 
	    			remain_recs -= need_to_insert;
	    			index += need_to_insert; 
	    			
	    			current_partition++;
	        		try { 
	        			table_i_meta.updateMeta(current_partition, rec_count_in_current_partition);  
	        			 
	        		}catch(Exception e) {
	        			System.out.println(e);
	        			resource_logger.info(Timer.currentTime() + " [EXCEPTION]: fail to update partition " + table_i_meta.getLatestPartition() + " metadata");
		    		 	
	        		}
	        		
	        		
				}
			}  
		}
		
		return patchResponseFromNodes(responses);
	} 
	
	/*
	 * @TaskHandlingMessage.addFunctionalColumn
	 */
	private ResponseCollector addFunctionalColumn(CMDEnumeration.command cmd, String[] params )
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
			int partition = controller_consts.parsePartitionNumber(partition_name);
			if(partition >=0 )
			{  
				String instance_name = master_map.get(partition_name);
				LunarDBClient client = instance_connection_map.get(instance_name);
				String[] new_param = null;
				switch(cmd)
				{
					case addFulltextColumn:
					case addStorableColumn:
					{
						new_param = new String[params.length];
						new_param[0] = params[0];
						new_param[1] = controller_consts.patchNameWithPartitionNumber(params[1], partition);
						new_param[2] = params[2];
					}
					break;
					case addAnalyticColumn:
					{
						new_param = new String[params.length];
						new_param[0] = params[0];
						new_param[1] = controller_consts.patchNameWithPartitionNumber(params[1], partition);
						new_param[2] = params[2];
						new_param[3] = params[3];
					}
					break; 
				}
	        	TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client,cmd,new_param );
	        	 
	        	Future<RemoteResult> resp = thread_executor.submit(tsqtn);
	        	responses.add(resp); 
			}
		}
		
		return patchResponseFromNodes(responses);
	}
	
	 
	private ResponseCollector ftQuery(String[] params )
	{
		/*
		 * <table name, remote result>
		 */
		ConcurrentHashMap<String, RemoteResult> response_map = new ConcurrentHashMap<String, RemoteResult>();
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		Iterator<String> keys = master_map.keySet().iterator();
		//int partition = controller_consts.parsePartitionNumber(latest_partition_name);
		String table = params[1];
		TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		int partition = table_i_meta.getLatestPartitionNumber() ; 
		
		for(int i=partition;i>=0;i--) {
			String current_partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name, i);
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
        	/*
        	RemoteResult resp_from_svr = tsqtn.call(); 
        	for(int j=0;j<resp_from_svr.getParams().length;j++)
     		{
     			System.out.println("LunarNode responded: "+ resp_from_svr.getParams()[j]);
     		}
     		*/
        	responses.add(resp); 
        	partition--;
		}
		
		return patchResponseFromNodes(responses);
	}
	
	private ResponseCollector fetchLog(String[] params)
	{
		/*
		 * <table name, remote result>
		 */
		ConcurrentHashMap<String, RemoteResult> response_map = new ConcurrentHashMap<String, RemoteResult>();
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		Iterator<String> keys = master_map.keySet().iterator();
		while(keys.hasNext())
		{
			String partition_name = keys.next();
			int partition = controller_consts.parsePartitionNumber(partition_name);
			if(partition >=0 )
			{  
				String instance_name = master_map.get(partition_name);
				LunarDBClient client = instance_connection_map.get(instance_name);
				
				CMDEnumeration.command cmd = CMDEnumeration.command.fetchLog; 
	        	
				String[] new_param = new String[params.length];
				new_param[0] = params[0];
				new_param[1] = controller_consts.patchNameWithPartitionNumber(params[1], partition);
	        	new_param[2] = params[2];
	        	new_param[3] = params[3];
	        	TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, 
	        															cmd, 
	        															new_param );
	        	 
	        	Future<RemoteResult> resp = thread_executor.submit(tsqtn);
	        	
	        	/*
	        	RemoteResult resp_from_svr = tsqtn.call(); 
	        	 for(int i=0;i<resp_from_svr.getParams().length;i++)
	     		{
	     			System.out.println("LunarNode responded: "+ resp_from_svr.getParams()[i]);
	     		}*/
	        	responses.add(resp); 
			}
		}
		
		return patchResponseFromNodes(responses);
	}
	
	private ResponseCollector fetchRecords(String[] params, boolean if_desc)
	{
		return fetchRecords(params[0], 
							params[1], 
							Long.parseLong(params[2].trim()), 
							Integer.parseInt(params[3].trim()), 
							if_desc); 
	}
	
	public long recsCount(String table)
	{
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		//int partition_i = controller_consts.parsePartitionNumber(latest_partition_name); 
		
		 
		TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		int partition_i = table_i_meta.getLatestPartitionNumber() ; 
		
		
		long total = 0;
		while(partition_i >=0 )
		{ 
			String latest_partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name, partition_i);
			
			String instance_name = master_map.get(latest_partition_name);
			LunarDBClient client = instance_connection_map.get(instance_name);
			CMDEnumeration.command cmd_for_rec_count = CMDEnumeration.command.recsCount; 
        	
			String[] new_param = new String[2];
			new_param[0] = getDBName();
			new_param[1] = controller_consts.patchNameWithPartitionNumber(table, partition_i); 
			TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, cmd_for_rec_count, new_param );
			Future<RemoteResult> resp = thread_executor.submit(tsqtn);
        	
        	responses.add(resp);
        	
        	partition_i--;
		}
		
		for(Future<RemoteResult> resp : responses)
		{
			if(resp != null)
			{
				RemoteResult mr;
				try {
					 mr = (RemoteResult)resp.get(); 
					 if(mr != null)
					 {
						 total += mr.getResultCount();
					 }
					 else
					 {
						System.err.println("no response @Resource.recsCount(String table)");
					 }
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} 
		return total;  
	}
	public ResponseCollector fetchRecords(String db, String table, long from, int count, boolean if_desc)
	{ 
		List<Future<RemoteResult>> responses = new ArrayList<Future<RemoteResult>>();
		
		TablePartitionMeta table_i_meta =  this.table_meta_map.get(table);
		int partition_i = table_i_meta.getLatestPartitionNumber() ; 
		
		 
		if(partition_i >=0 )
		{ 
			String latest_partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name, partition_i);
			
			String instance_name = master_map.get(latest_partition_name);
			LunarDBClient client = instance_connection_map.get(instance_name);
			CMDEnumeration.command cmd_for_rec_count = CMDEnumeration.command.recsCount; 
        	
			String[] new_param = new String[2];
			new_param[0] = db;
			new_param[1] = controller_consts.patchNameWithPartitionNumber(table, partition_i); 
			TaskSendReqestToNode tsqtn = new TaskSendReqestToNode( client, cmd_for_rec_count, new_param );
			RemoteResult rr = tsqtn.call();
			int rec_count_in_partition_i = 0;
			if(rr.isSucceed())
				rec_count_in_partition_i = rr.getResultCount() ;
			 	
			
			/*
			 * recs in each partition:
			 * |__2000_______|  |__2000_______|  |__2000_______| ... |___750_____|
			 * partition_0        partition_1     partition_2   ...   latest_partition
			 *                              from+count -------------------^ from position
			 */
			int begin_in_which_partition =  partition_i;
			int i_th_rec_count = rec_count_in_partition_i;
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
					 * move to the previous partition.
					 */
					begin_in_which_partition --;
					if(begin_in_which_partition < 0)
						return null;
								
					i_th_rec_count =  max_recs_per_partition.get();
				} 
			} 
			
			i_from += i_th_rec_count;
			 
			int g_remains = count;
			while(g_remains > 0 && begin_in_which_partition >= 0)
			{
				int fetch_ith_iter = (i_th_rec_count - (int)i_from) >= g_remains? g_remains : (i_th_rec_count - (int)i_from); 
				CMDEnumeration.command cmd = CMDEnumeration.command.fetchRecordsDESC; 
		        	
					String[]  param_fetching = new String[4];
					param_fetching[0] = db;
					param_fetching[1] = controller_consts.patchNameWithPartitionNumber(table, begin_in_which_partition);
					param_fetching[2] = ""+i_from;
					param_fetching[3] = ""+fetch_ith_iter;
		        	TaskSendReqestToNode request_fetching_recs = new TaskSendReqestToNode( 
		        															client, 
		        															cmd, 
		        															param_fetching );
		        	 
		        	Future<RemoteResult> resp = thread_executor.submit(request_fetching_recs);
		        	
		        	responses.add(resp); 
		        	
		        	g_remains -= fetch_ith_iter;
				 
		        	/*
					 * seek previous partition for more records
					 */
		        	begin_in_which_partition --;
		        	if(begin_in_which_partition < 0)
		        	{
		        		return patchResponseFromNodes(responses);
		        	}
		        	i_from = 0;
		        	i_th_rec_count = max_recs_per_partition.get(); 
			} 	
		}
	 
		
		return patchResponseFromNodes(responses);
	}
	
	private ResponseCollector sqlSelect(String statement)
	{   
		
		ResultSet result = executeSqlSelect(statement);
		long total = 0;
		
		/* 
		try {
		 
			if(result.last())
			{
				total = result.getRow();
				result.beforeFirst();
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		*/
		return new ResponseCollector(this, result, total);
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
		
		//int partition = controller_consts.parsePartitionNumber(latest_partition_name);
		
		for(int i=partition;i>=0;i--) {
			String current_partition_name = controller_consts.patchNameWithPartitionNumber(this.resource_name, i);
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
		}
		
		return patchResponseFromNodes(responses);
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
	
	public String getDBName( )
	{
		return this.resource_name;
	}
	
	 
}
