
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
package lunarion.node;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType; 
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.spectator.RoutingTableProvider;

import LCG.DB.API.LunarDB;
import lunarion.cluster.quickstart.Quickstart;
import lunarion.cluster.utile.ControllerConstants;
import lunarion.cluster.utile.Screen;

public class LunarNode {
	private Logger logger = Logger.getLogger("LunarNode"); 
	
	
	private final String node_name; /* is as this form: 192.168.1.8_30001*/
	
	private String db_root_path;
	private String db_name;
	//private final LunarDB db_node_instance;
	private HelixManager manager = null;
	private int node_port;
	private String ZK_ADDRESS;
	private String cluster_name;
	private String resource_name;/* used as db name*/
	
	/* actually it is a master-slave model*/
	private static final String STATE_MODEL_NAME = ControllerConstants.STATE_MODEL_NAME;
	
	protected  ExecutorService thread_executor = Executors.newFixedThreadPool(1); 
	LunarServerStandAlone lsa = new LunarServerStandAlone(); 
	
	
	
	public LunarNode(String _node_ip, int _node_port, String _zookeeper_addr, 
					String _data_root,
					String _cluster_name,  
					String _resource_name, 
					String _db_creation_conf_file)  
	{
		 
		InstanceConfig instanceConfig = new InstanceConfig(_node_ip + "_" + _node_port);
	    instanceConfig.setHostName(_node_ip);
	    instanceConfig.setPort("" + _node_port);
	    instanceConfig.setInstanceEnabled(true);
	    Screen.echo("[INFO]: node add myself "+instanceConfig.getInstanceName()+" to the cluster :" +  _cluster_name);
	    String instanceName = instanceConfig.getInstanceName();
	    node_name = instanceName;
		
		
		ZK_ADDRESS = _zookeeper_addr;
		node_port = _node_port;
		cluster_name = _cluster_name;
		resource_name = _resource_name;
		 LunarDB db_node_instance = new LunarDB();
		db_name = resource_name;
				
		db_root_path = _data_root+"/"+_cluster_name +"/" + node_name +"/" ;
		new File(db_root_path).mkdirs();
		
		if(!db_node_instance.createDB(db_root_path, _db_creation_conf_file))
		{
			System.out.println("[WARNING]: database " + db_root_path + " exists on this node. Can not create again. "); 
		} 
		else
		{
			System.out.println("[SUCCEED]: database " + db_root_path + " has been created successfully. "); 
		} 
		 
		try {
			db_node_instance.closeDB();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	 public void start() throws InterruptedException  
	 {
	     /* 
		 if(!db_node_instance.openDB(db_root_path+db_name))
		 {
			 System.out.println("[ERROR]: fail to open db at: " + db_root_path); 
			 return;
		 }
		 */
		 try {
			 lsa.startServer(db_root_path ); 
			 
			 TaskStartWaiting task_sw = new TaskStartWaiting(lsa,node_port );
			 thread_executor.submit(task_sw) ;
			// LunarServerStandAlone.getInstance().bind(node_port);
				 
	     }
		 catch(Exception e)
		 {
			 e.printStackTrace();
			 logger.severe("[NODE ERROR]: unable to start db at: " + db_root_path);  
		 }
	        
	      boolean alive = true;
	      boolean connected = false;
	      while(alive)
	      {
	    	  Thread.sleep(5000);
	    	  
	    	  while(!connected)
	    	  {
	    		  try
	    		  {
	    			  System.out.println("[TRY]: try to connect to the coordinator"); 
	    			  /*
	    			   * every time we connect to the coordinator, we need to get a  
	    			   * new pointer of the HelixManager from HelixManagerFactory, 
	    			   * who communicates with the coordinator of the latest information, 
	    			   * including zookeeper status, resource status and so on.
	    			   */
	    			  manager = HelixManagerFactory.getZKHelixManager(cluster_name, node_name,
	    			              InstanceType.PARTICIPANT, ZK_ADDRESS);
	    			  
	    			  MasterSlaveStateModelFactory stateModelFactory =
	    			          new MasterSlaveStateModelFactory(LunarServerStandAlone.getInstance(), 
	    			        		  								db_name, 
	    			        		  								manager, 
	    			        		  								node_name, 
	    			        		  								node_port,
	    			        		  								10);

	    			   
	    			  StateMachineEngine stateMach = manager.getStateMachineEngine();
	    			  stateMach.registerStateModelFactory(STATE_MODEL_NAME, stateModelFactory);
	    			  
	    			  manager.connect(); 
	    			  
	    		      
	    			  connected = true;
	    		  }
	    		  catch(Exception e)
	    		  {
	    			  e.printStackTrace();
	    		  }
	    	  }
	    	  
	    	  if(!manager.isConnected())
	    		  connected = false;
	    	  else
	    		  //System.out.println("[INFO]: connection is alive for instance: " + node_name); 
	    		  ;
			  
	      }
	      
	      
	      
	}

	public void stop() 
	{ 
		/*
		if(this.db_node_instance != null)
		{
			db_node_instance.save();
			try {
				db_node_instance.closeDB();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	 */
		
		if(lsa != null)
			lsa.closeServer(); 
    
		if (manager != null) 
			manager.disconnect();
	}
	
	public static void main(String[] args) {
	    
		  /*
		   * use the zookeeper and Helix admin in StartCoordinator
		   */
		String zkAddr = "localhost:2199";
		String data_root = "/home/feiben";
		String cluster_name = "DBCluster"; 
		String resource_name = "RTSeventhDB";
		 
		String node_ip = "localhost";
		 
		String creation_conf = "/home/feiben/EclipseWorkspace/LunarBaseApplication/creation.conf";
		 
		int port = 30000 + 1;
		if (args.length < 7) {
			System.err.println("USAGE: LunarNode zk_address data_root cluster_name resource_name node_ip node_port creation_conf");
			System.err.println("[EXAMPLE]: LunarNode 127.0.0.1:2199 my_cluster my_db_name 127.0.0.1 30001 /home/creation.conf");
			System.err.println("[ATTENTION]: resource_name must be the database_name in the file creation.conf.");
			System.err.println("[ATTENTION]: make sure you have the permition to create directory under the data_root you specify.");
			
		}
		else
		{
			zkAddr = args[0];
			data_root = args[1];
			cluster_name = args[2]; 
			resource_name = args[3];
			node_ip = args[4];
			port = Integer.parseInt(args[5]);
			creation_conf = args[6];
		}
		LunarNode ln = new LunarNode(node_ip, port, zkAddr, data_root, cluster_name, resource_name, creation_conf);
		 
		  
		try{
			ln.start(); 
			Thread.currentThread().join();
		} catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally 
		{
			ln.stop();
	    }
	  }

}
