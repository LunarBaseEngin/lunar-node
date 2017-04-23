package lunarion.cluster.quickstart;

import java.util.List;

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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext; 
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.spectator.RoutingTableProvider;

public class DummyParticipant {
  // dummy master-slave state model
  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "ERROR"
  })
  public static class DummyMSStateModel extends StateModel {
    @Transition(to = "SLAVE", from = "OFFLINE")
    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes SLAVE from OFFLINE for " + partitionName);
    }

    @Transition(to = "MASTER", from = "SLAVE")
    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes MASTER from SLAVE for " + partitionName);
    }

    @Transition(to = "SLAVE", from = "MASTER")
    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes SLAVE from MASTER for " + partitionName);
    }

    @Transition(to = "OFFLINE", from = "SLAVE")
    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes OFFLINE from SLAVE for " + partitionName);
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes DROPPED from OFFLINE for " + partitionName);
    }

    @Transition(to = "OFFLINE", from = "ERROR")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      String partitionName = message.getPartitionName();
      String instanceName = message.getTgtName();
      System.out.println(instanceName + " becomes OFFLINE from ERROR for " + partitionName);
    }

    @Override
    public void reset() {
      System.out.println("Default MockMSStateModel.reset() invoked");
    }
  }

  // dummy master slave state model factory
  public static class DummyMSModelFactory extends StateModelFactory<DummyMSStateModel> {
    @Override
    public DummyMSStateModel createNewStateModel(String resourceName, String partitionName) {
      DummyMSStateModel model = new DummyMSStateModel();
      return model;
    }
  }

  public static void main(String[] args) {
    
	  /*
	if (args.length < 3) {
      System.err.println("USAGE: DummyParticipant zkAddress clusterName instanceName");
      System.exit(1);
    }

    String zkAddr = args[0];
    String clusterName = args[1];
    String instanceName = args[2];
	   */
	 
	  /*
	   * use the zookeeper and Helix admin in Quickstart
	   */
	  String zkAddr = "localhost:2199"; 
	    String clusterName = "HELIX_QUICKSTART";
	    int port = 12000 + 6;
	    InstanceConfig instanceConfig = new InstanceConfig("localhost_" + port);
	    instanceConfig.setHostName("localhost");
	    instanceConfig.setPort("" + port);
	    instanceConfig.setInstanceEnabled(true);
	    Quickstart.echo("NODE add myself to the cluster :" + instanceConfig.getInstanceName() );
	    String instanceName = instanceConfig.getInstanceName();
	    
    HelixManager manager = null;
    try {
      manager =
          HelixManagerFactory.getZKHelixManager(clusterName, instanceName,
              InstanceType.PARTICIPANT, zkAddr);

      /*
      StateMachineEngine stateMach = manager.getStateMachineEngine();
      DummyMSModelFactory msModelFactory = new DummyMSModelFactory();
      stateMach.registerStateModelFactory("MasterSlave", msModelFactory);

      manager.connect();
      */
      /*
       * keeps the statemodel the same to what node is in Quickstart
       */
      ExampleMasterSlaveStateModelFactory stateModelFactory =
              new ExampleMasterSlaveStateModelFactory(instanceName);
      String STATE_MODEL_NAME = "MyStateModel";
          StateMachineEngine stateMach = manager.getStateMachineEngine();
          stateMach.registerStateModelFactory(STATE_MODEL_NAME, stateModelFactory);
          manager.connect();

          String RESOURCE_NAME = "MyResource";

          for(int i=0;i<10;i++)
          {
	          Thread.sleep(10000);
	          
	          RoutingTableProvider routingTableProvider = new RoutingTableProvider();
	          manager.addExternalViewChangeListener(routingTableProvider);
	
	          String resurce_name = RESOURCE_NAME;
	          String partition_name = RESOURCE_NAME+"_4";
	          List<InstanceConfig>  instances = routingTableProvider.getInstances(resurce_name, partition_name, "MASTER");
	 
	          if (instances.size() > 0) {
	        	  if(instances.size() == 1) 
	        	  {
	        		  InstanceConfig newMasterConfig = instances.get(0);
	        		  String master = newMasterConfig.getInstanceName();
	        		  System.out.println("Found new master:" + newMasterConfig.getInstanceName());
	        			  
	        		 // startReplication(newMasterConfig);
	        		 
	        		  
	        		  System.out.println("Already replicating from " + master);
	        		  
	        	  } 
	        	  else 
	        	  {
	        		  System.out.println("Invalid number of masters found:" + instances);
	        	  }
	          } 
	          else 
	          {
	        	  System.out.println("No master found");
	          }
	          //InstanceConfig  theInstance = instances.get(0);  // should choose an instance and throw an exception if none are available
	         // Quickstart.echo("partion " + partition_name + " with the master : " + theInstance.getInstanceName());
	          
          }

      Thread.currentThread().join();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (manager != null) {
        manager.disconnect();
      }
    }
  }
}
