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
  LunarDBServerStandAlone db_server;
  //LunarDB db_node_instance = null;
  HelixManager manager = null;
  
  String instance_name = "";//192.168.0.88:30001
  String resource_name = "";//used as db name
  int node_port;
    

  public MasterSlaveStateModelFactory(  LunarDBServerStandAlone  _db_server,
		  								String _db_name,
		  								HelixManager _manager, 
		  								String _instance_name,  
		  								int _node_port,
		  								int _delay) 
  {
	  db_server = _db_server;
	  //db_node_instance = _db_server.getDBInstant(_db_name);
	  manager = _manager; 
	  instance_name = _instance_name;
	  resource_name = _db_name;
	  delay = _delay;
	  node_port = _node_port;
  } 

  @Override
  public StateModel createNewStateModel(String _resource_name, String _partition_name) {
	  resource_name  = _resource_name;
	   
	  MasterSlaveStateModel stateModel = new MasterSlaveStateModel( db_server.getDBInstant(resource_name),
			  														manager, 
			  														instance_name, 
			  														resource_name, 
			  														_partition_name);
	  
	   
	  stateModel.setDelay(delay); 
	    
	  return stateModel;
  }

 }
