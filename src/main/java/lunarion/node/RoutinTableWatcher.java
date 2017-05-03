
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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.NotificationContext;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.log4j.Logger;

import LCG.DB.API.LunarDB;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.logger.LoggerFactory;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;

public class RoutinTableWatcher extends RoutingTableProvider {
	
	private String instance_name;
	private String resource_name; 
	private String partition_name; 
	private InstanceConfig current_master_config = null;
	
	private AtomicBoolean i_am_master = new AtomicBoolean(false);
	/*
	 * every partition has its own logger.
	 */ 
	private Logger partition_logger = null;
	/*
	 * use to connect to the master for data replication.
	 */
	/*
	 * local db instance that replicates data from the master
	 */
	//private final LunarDB local_db;
	
	private LunarDBServerStandAlone db_server; 
	//private LunarDBClient client ;
	private TaskReplication replication_service;
	private AtomicBoolean replication_started = new AtomicBoolean(false);
	private AtomicBoolean replication_initiated = new AtomicBoolean(false);
	private AtomicBoolean in_finding_master = new AtomicBoolean(false);
	
	
	private ExecutorService thread_executor = Executors.newFixedThreadPool(1); 
	public AtomicBoolean stop_singnal = new AtomicBoolean(false);
	public AtomicBoolean still_running = new AtomicBoolean(false);
	
	public RoutinTableWatcher(String _instance_name, String _resource_name, 
								String _partion_name, LunarDBServerStandAlone  _db_server) {
		this.instance_name = _instance_name;
		this.resource_name = _resource_name; 
		this.partition_name = _partion_name; 
		this.partition_logger = LoggerFactory.getLogger( _instance_name + "_ "+ _partion_name);
		//this.local_db = _local_db;
		this.db_server = _db_server;
		//this.client = new LunarDBClient();
	}
	
	public void startReplication()
	{ 
		replication_initiated.set(true);
		
		if(!findNewMaster())
			return; 
		
		if(replication_started.get())
		{
			//replication_service.stopRep();
			//
			if(replication_service!=null)
			{
				replication_service.shutdown();
				this.stop_singnal.set(true);
				//client.shutdown();
				//thread_executor.shutdownNow();
			}
			
			//thread_executor.shutdownNow(); 
			// client.shutdown();
			//replication_started.set(false);
		}  
		
		if(i_am_master.get())
		{
			System.out.println(" @RoutinTableWatcher.startReplication(), I'm the master of " + partition_name + ", nothing to replicate.");
			partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @startReplication(), I'm the master of " + partition_name + ", nothing to replicate.");
			replication_started.set(false);
			if(replication_service!=null)
			{ 
				replication_service.shutdown();
				this.stop_singnal.set(true);
				//client.shutdown();
				//thread_executor.shutdownNow();
			}
			//thread_executor.shutdownNow();
			return;
		} 
		
		//while(this.still_running.get())
			//;
		
		this.stop_singnal.set(false);
		System.out.println(" @RoutinTableWatcher.startReplication(),  Start replicating data from the master of partition: " + partition_name);
		 partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @RoutinTableWatcher.startReplication(), Start replicating data from the master of partition: " + partition_name);
    	 
		 LunarDBClient client = new LunarDBClient();
		try {
   		 
			client.connect(current_master_config.getHostName(), LunarNode.calcDBPort(Integer.parseInt(current_master_config.getPort())) );
		} 
		catch (Exception e) 
		{
			client.shutdown();
			 partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @RoutinTableWatcher.startReplication(), " + " can not connect to the master " + resource_name + " for partition:" + partition_name );
		  
			e.printStackTrace();
			return; 
		}	 
    	 
		if(client.isConnected())
		{
    		System.out.println("@RoutinTableWatcher.startReplication(), Master of partition: "+ partition_name + " is connected.");
    		partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @RoutinTableWatcher.startReplication(), Master of partition: "
    							+ partition_name 
    							+ "(" + current_master_config.getHostName() 
    							+ "_"
    							+ LunarNode.calcDBPort(Integer.parseInt(current_master_config.getPort()))
    							+ ") is connected.");
		
    		replication_service = new TaskReplication( instance_name, db_server, client,
													current_master_config.getHostName(), 
													LunarNode.calcDBPort(Integer.parseInt(current_master_config.getPort())) , 
													partition_logger,
													partition_name,
													resource_name);
		
    		//replication_service.startRep();
    		thread_executor.submit(replication_service);
    		replication_started.set(true);
		}
		else
		{
			System.out.println("@RoutinTableWatcher.startReplication(), Master of partition: "+ partition_name + " can not connect to.");
    		partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @RoutinTableWatcher.startReplication(), Master of partition: "
    							+ partition_name 
    							+ "(" + current_master_config.getHostName() 
    							+ "_"
    							+ LunarNode.calcDBPort(Integer.parseInt(current_master_config.getPort()))
    							+ ") can not connect to.");
    		replication_started.set(false);
		}
	}
	
	public void stopReplication()
	{ 
		if(replication_started.get())
		{
			this.stop_singnal.set(true);
			//replication_service.stopRep();
			if(replication_service!=null)
				replication_service.shutdown();
			
			//thread_executor.shutdownNow();
			//client.shutdown();
			replication_started.set(false);
		}  
		replication_initiated.set(false);
		
	}
	private boolean findNewMaster()
	{
		//while(in_finding_master.get())
		//{
		//	System.err.println("==========waiting for finding master================");
		//}
		
		in_finding_master.set(true);
		List<InstanceConfig>  instances = getInstancesForResource(resource_name,partition_name, "MASTER");

		if (instances.size() > 0) 
		{
	    	  if(instances.size() == 1) 
	    	  {
	    		  InstanceConfig newMasterConfig = instances.get(0);
	    		  String master = newMasterConfig.getInstanceName();
	    		  
	    		  if(master.equals(this.instance_name))
	    		  {
	    			  System.out.println("At instance " + this.instance_name);
	    			  System.out.println("I am the master of resource " + resource_name + " for partition:" + partition_name); 
	    			  i_am_master.set(true);
	    			 
	    			   partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @findMaster(), " + "At instance " + this.instance_name + ", I am the master of resource " + resource_name + " for partition:" + partition_name);
	    			  in_finding_master.set(false);
	    			  if(current_master_config != null)
	    			  { 
	    				  current_master_config = null;
	    				  return true;
	    			  }
	    			  else 
	    			  { 
	    				  return false;
	    			  }
	    		  }
	    		  else
	    		  {	    			  
	    			  if(current_master_config == null 
	    					  || ! current_master_config.getInstanceName().equalsIgnoreCase(newMasterConfig.getInstanceName()))
	    			  {
	    				  current_master_config = newMasterConfig; 
		    			  i_am_master.set(false);
		    			  
		    			  System.out.println("Found new master " + current_master_config.getInstanceName() + " of resource " + resource_name + " for partition:" + partition_name);
		    			   partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @findMaster(), " + "Found new master " + current_master_config.getInstanceName() + " of resource " + resource_name + " for partition:" + partition_name);
		    			  in_finding_master.set(false);
		    			  return true; 
	    			  }
	    			  else
	    			  {
	    				  in_finding_master.set(false);
	    				  i_am_master.set(false);
	    				  return false;
	    			  }
	    		  }
	    	  } 
	    	  else 
	    	  {
	        		System.out.println("Invalid number of masters found at :" + instances);
	        		i_am_master.set(false);
	        		 partition_logger.info(Timer.currentTime()+ " [NODE ERROR]: @findMaster(), Invalid number of masters found at :" + instances);
	        		in_finding_master.set(false);
	        		return false; 
	    	  }
	      } 
	      else 
	      {
	    	  System.out.println(" @findMaster(), No master of resource " + resource_name + " for partition "+ partition_name +" found");
	    	  i_am_master.set(false);
	    	   partition_logger.info(Timer.currentTime()+ " [NODE WARNING]: @findMaster(), No master of resource " + resource_name + " for partition "+ partition_name +" found");
	    	  in_finding_master.set(false);
	    	  return false;
	      } 
		
		 
	     	 
	}
	
	
	@Override
	public void onExternalViewChange(List<ExternalView> viewList, NotificationContext context) {
			super.onExternalViewChange(viewList, context);
			
			 partition_logger.info(Timer.currentTime()+ " [COORDINATOR STATE CHANGE]: @onExternalViewChange(...)=============================");
		    	
			//findNewMaster();
			/*
			 * must after this partition from offline to slave, then we can do findMaster 
			 * or replication things.
			 * 
			 * the replication_initiated is the first time set true at 
			 * MasterSlaveStateModel.onBecomeSlaveFromOffline(...)
			 */
			if(replication_initiated.get())
			{ 
				 
				startReplication();
			}
	}
}
