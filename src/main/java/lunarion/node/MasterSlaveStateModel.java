
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

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;

import LCG.DB.API.LunarDB;

/*
 * one for every partion of every resource.
 */
public class MasterSlaveStateModel extends StateModel {
  
	 
	//private LunarDB local_db;
	private LunarDBServerStandAlone db_server; 
	private HelixManager manager = null;
	private RoutinTableWatcher routing_table_provider = null;
	     
	private int trans_delay = 0;
  
	private String partition_name;
	private String instance_name = "";
	private String resoure_name = "";
  
  

public MasterSlaveStateModel(LunarDBServerStandAlone  _db_server, HelixManager _manager, String _instance, String _resource, String _partition) 
{
	
	//local_db = _local_db;
	db_server = _db_server;
	  manager = _manager;
	  instance_name = _instance;
	  resoure_name = _resource;
	  partition_name = _partition;

	  this.routing_table_provider = new RoutinTableWatcher(instance_name, resoure_name, partition_name, db_server);
  	
	  try {
		  manager.addExternalViewChangeListener(routing_table_provider);
	  } 
	  catch (Exception e) 
	  {
  		e.printStackTrace();
	  }
  }
  
  public String getPartitionName() {
  	return partition_name;
  }

  
  
  public void setDelay(int delay) {
  	trans_delay = delay > 0 ? delay : 0;
  } 

  public void onBecomeSlaveFromOffline(Message message, NotificationContext context) throws InterruptedException {
    System.out.println(instance_name + " transitioning from " + message.getFromState() + " to "
        + message.getToState() + " for " + partition_name);
   
    routing_table_provider.startReplication();
    Thread.sleep(3000);
   
  }

  private void sleep() {
    try {
      Thread.sleep(trans_delay);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
    System.out.println(instance_name + " transitioning from " + message.getFromState() + " to "
        + message.getToState() + " for " + partition_name); 
       
    routing_table_provider.startReplication();
    sleep();

  }

  public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
    System.out.println(instance_name + " transitioning from " + message.getFromState() + " to "
        + message.getToState() + " for " + partition_name);
    
	routing_table_provider.stopReplication();
    
    sleep();

  }

  public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
    System.out.println(instance_name + " transitioning from " + message.getFromState() + " to "
        + message.getToState() + " for " + partition_name);
    sleep();

  }

  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    System.out.println(instance_name + " Dropping partition " + partition_name);
    sleep();

  }
}

