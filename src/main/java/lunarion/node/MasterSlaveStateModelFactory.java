package lunarion.node;

import java.util.List;

import org.apache.helix.HelixManager;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.NotificationContext;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.spectator.RoutingTableProvider;

import LCG.DB.API.LunarDB;

@SuppressWarnings("rawtypes")
public class MasterSlaveStateModelFactory extends StateModelFactory<StateModel> {
  int delay;
  LunarServerStandAlone db_server;
  //LunarDB db_node_instance = null;
  HelixManager manager = null;
  
  String instance_name = "";//192.168.0.88:30001
  String resource_name = "";
  int port;
    

  public MasterSlaveStateModelFactory(  LunarServerStandAlone  _db_server,
		  								String _db_name,
		  								HelixManager _manager, 
		  								String _instance_name,  
		  								int _port,
		  								int _delay) 
  {
	  db_server = _db_server;
	  //db_node_instance = _db_server.getDBInstant(_db_name);
	  manager = _manager; 
	  instance_name = _instance_name;
	  resource_name = _db_name;
	  delay = _delay;
	  port = _port;
  } 

  @Override
  public StateModel createNewStateModel(String _resource_name, String _partition_name) {
	  resource_name  = _resource_name;
	   
	  MasterSlaveStateModel stateModel = new MasterSlaveStateModel( manager, 
			  														instance_name, 
			  														resource_name, 
			  														_partition_name);
	  
	  stateModel.setDBServer(db_server, port ); 
	  stateModel.setDelay(delay); 
	    
	  return stateModel;
  }

  public static class MasterSlaveStateModel extends StateModel {
    
	//private LunarDB db_node_instance;
	  private LunarServerStandAlone db_server;
	private int port;
	private HelixManager manager = null;
	private RoutinTableWatcher routing_table_provider = null;
	     
	private int trans_delay = 0;
    private String partition_name;
    private String instance_name = "";
    private String resoure_name = "";
    
    
    public MasterSlaveStateModel(HelixManager _manager, String _instance, String _resource, String _partition) 
    {
    	manager = _manager;
    	instance_name = _instance;
    	resoure_name = _resource;
    	partition_name = _partition;
    	
    	this.routing_table_provider = new RoutinTableWatcher(instance_name, resoure_name, partition_name);
    	try {
    		manager.addExternalViewChangeListener(routing_table_provider);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
    
    public String getPartitionName() {
    	return partition_name;
    }

    public void setDBServer(LunarServerStandAlone _db_server, 
    						int _port )
    {
    	//this.db_node_instance = _db_node_instance;
    	db_server = _db_server;
    	this.port = _port; 
    } 
    
    public void setDelay(int delay) {
    	trans_delay = delay > 0 ? delay : 0;
    } 

    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) throws InterruptedException {
      System.out.println(instance_name + " transitioning from " + message.getFromState() + " to "
          + message.getToState() + " for " + partition_name);
      /*
       * blocked, if replication does not complete, this partition will never be a master.
       */
      routing_table_provider.replicateFromMaster();
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
         
      sleep();

    }

    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      System.out.println(instance_name + " transitioning from " + message.getFromState() + " to "
          + message.getToState() + " for " + partition_name);
      
      
      
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

}
