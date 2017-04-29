
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
	
	private AtomicBoolean who_am_i = new AtomicBoolean(false);
	/*
	 * every partition has its own logger.
	 */ 
	private Logger partition_logger = null;
	/*
	 * use to connect to the master for data replication.
	 */
	private final LunarDBClient client = new LunarDBClient();
	/*
	 * local db instance that replicates data from the master
	 */
	private final LunarDB local_db;
	 
	public RoutinTableWatcher(String _instance_name, String _resource_name, 
								String _partion_name, LunarDB _local_db ) {
		this.instance_name = _instance_name;
		this.resource_name = _resource_name; 
		this.partition_name = _partion_name; 
		this.partition_logger = LoggerFactory.getLogger( _instance_name + "_ "+ _partion_name);
		this.local_db = _local_db;
	}
	
	public void findMaster()
	{
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
	    			  who_am_i.set(true);
	    			  if( client.isConnected())
	    				  client.shutdown();
	    			  partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @findMaster(), " + "At instance " + this.instance_name + ", I am the master of resource " + resource_name + " for partition:" + partition_name); 
	    		  }
	    		  else
	    		  {
	    			  current_master_config = newMasterConfig; 
	    			  System.out.println("Found new master " + current_master_config.getInstanceName() + " of resource " + resource_name + " for partition:" + partition_name);
	    			  partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @findMaster(), " + "Found new master " + current_master_config.getInstanceName() + " of resource " + resource_name + " for partition:" + partition_name);
		    		  
	    			  if( client.isConnected())
	    				 client.shutdown();
	    			 
	    			  try {
						client.connect(current_master_config.getHostName(), 
								  		Integer.parseInt(current_master_config.getPort()));
	    			  } catch (Exception e) {
						client.shutdown();
						partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @findMaster(), " + " can not connect to the master " + resource_name + " for partition:" + partition_name );
			    		  
						e.printStackTrace();
						return;
	    			  }
	    			  
	    			  partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @findMaster(), " + "master of resource " + resource_name + " for partition:" + partition_name + " has been connected");
		    		  
	    		  }
	    	  } 
	    	  else 
	    	  {
	        		System.out.println("Invalid number of masters found at :" + instances);
	        		if( client.isConnected())
	    				 client.shutdown();
	        		partition_logger.info(Timer.currentTime()+ " [NODE ERROR]: @findMaster(), Invalid number of masters found at :" + instances);
		    		  
	    	  }
	      } 
	      else 
	      {
	    	  System.out.println(" @findMaster(), No master of resource " + resource_name + " for partition "+ partition_name +" found");
	    	  if( client.isConnected())
 				 client.shutdown();
	    	  partition_logger.info(Timer.currentTime()+ " [NODE WARNING]: @findMaster(), No master of resource " + resource_name + " for partition "+ partition_name +" found");
	    		
	      } 
	     	 
	}
	
	public void replicateFromMaster( ) throws InterruptedException
	{
		System.out.println(" @replicateFromMaster(),  Start replicating data from the master of partition: " + partition_name);
		partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), Start replicating data from the master of partition: " + partition_name);
    	
		if(who_am_i.get())
		{
			System.out.println(" @replicateFromMaster(), I'm the master of " + partition_name + ", nothing to replicate.");
			partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), I'm the master of " + partition_name + ", nothing to replicate.");
			return;
		}
		
		if(client.isConnected())
 		{
 			System.out.println(" @replicateFromMaster(), master of partition: " + partition_name + " is connected.");
 	 		
 			int partition_number = ControllerConstants.parsePartitionNumber(partition_name);
 			
 			CMDEnumeration.command cmd = CMDEnumeration.command.fetchTableNamesWithSuffix;
        	String[] params = new String[2];
        	params[0] = resource_name;  
        	params[1] = ControllerConstants.patchPartitionLogSuffix( partition_number);
        	
        	MessageResponse resp_from_svr = client.sendRequest(cmd, params); 
 			if(resp_from_svr.isSucceed())
 			{
 				System.out.println("LunarNode responded command: "+ resp_from_svr.getCMD());
 	    		System.out.println("LunarNode responded UUID: "+ resp_from_svr.getUUID());
 	    		System.out.println("LunarNode responded suceed: "+ resp_from_svr.isSucceed());
 	    		for(int i=0;i<resp_from_svr.getParams().length;i++)
 	    		{
 	    			System.out.println("LunarNode responded: "+ resp_from_svr.getParams()[i]);
 	    		}
 			}
 			else
 			{
 				partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), no table for partition: " + partition_name + " of resource " + resource_name + " exist.");
 		    	
 			}
 		}
		else
		{
			System.out.println("@replicateFromMaster(), Master of partition: "+ partition_name + " is not connected.");
			partition_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), Master of partition: "+ partition_name + " is not connected.");
	    	
		}
	}
	@Override
	public void onExternalViewChange(List<ExternalView> viewList, NotificationContext context) {
			super.onExternalViewChange(viewList, context);
			
			partition_logger.info(Timer.currentTime()+ " [COORDINATOR STATE CHANGE]: @onExternalViewChange(...)=============================");
		    	
			findMaster();
		 
			/*
			try {
				replicateFromMaster( );
			} catch (InterruptedException e) {
				partition_logger.info(Timer.currentTime()+ " [NODE EXCEPTION]: thread interrupted when repliacting from master of partition " + partition_name + ".");
 		    	
				e.printStackTrace();
			} */
	}
}
