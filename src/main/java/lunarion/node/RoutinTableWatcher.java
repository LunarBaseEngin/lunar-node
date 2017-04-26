
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

import org.apache.helix.NotificationContext;
import org.apache.helix.spectator.RoutingTableProvider;

import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;

public class RoutinTableWatcher extends RoutingTableProvider {
	
	String instance_name;
	String resource_name; 
	String partition_name; 
	private InstanceConfig current_master_config = null;
	
	/*
	 * use to connect to the master for data replication.
	 */
	private LunarDBClient client = new LunarDBClient();
	 
	public RoutinTableWatcher(String _instance_name, String _resource_name, String _partion_name ) {
		this.instance_name = _instance_name;
		this.resource_name = _resource_name; 
		this.partition_name = _partion_name; 
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
	    		  }
	    		  else
	    		  {
	    			  current_master_config = newMasterConfig; 
	    			  System.out.println("Found new master of resource " + resource_name + " for partition:" + partition_name);
	    			  if(client.isConnected())
	    				 client.shutdown();
	    			 
	    			  try {
						client.connect(current_master_config.getHostName(), 
								  		Integer.parseInt(current_master_config.getPort()));
	    			  } catch (Exception e) {
						client.shutdown();
						e.printStackTrace();
	    			  }
	    		  }
	    	  } 
	    	  else 
	    	  {
	        		System.out.println("Invalid number of masters found:" + instances);
	    	  }
	      } 
	      else 
	      {
	    	  System.out.println("No master of resource " + resource_name + " for partition "+ partition_name +" found");
	      } 
	     	 
	}
	
	public void replicateFromMaster( )
	{
		System.out.println("Start replicating data from the master of partition: " + partition_name);
 		if(client.isConnected())
 		{
 			System.out.println("master of partition: " + partition_name + " is connected.");
 	 		
 			int partition_number = ControllerConstants.parsePartitionNumber(partition_name);
 			
 			
 			
 		}
	}
	@Override
	public void onExternalViewChange(List<ExternalView> viewList, NotificationContext context) {
			super.onExternalViewChange(viewList, context);
			
			findMaster();
			replicateFromMaster( );
	}
}
