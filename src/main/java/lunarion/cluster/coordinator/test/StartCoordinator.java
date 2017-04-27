
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
package lunarion.cluster.coordinator.test;

import java.io.File;
import java.util.List;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;

import lunarion.cluster.coordinator.Coordinator;
import lunarion.node.utile.ControllerConstants;
import lunarion.node.utile.Screen;

public class StartCoordinator { 
	
	
	public static void main(String[] args) throws Exception {
		
		/*
		 * by default, for testing only.
		 */
		String zkAddr = "localhost:2199";
		String cluster_name = "DBCluster";
		String resource_name = "RTSeventhDB"; /* used as db name */
		int num_partition = 6;
		int num_replicas = 2;
		
		 if (args.length < 5) {
		      System.err.println("[USAGE]: StartCoordinator _zk_ddress _cluster_name _resource_name _num_partiotions _num_replicas");
		      System.err.println("[EXAMPLE]: Start 127.0.0.1:2199 my_cluster my_resource 6 2");
		      
		      //System.exit(1);
		    }
		 else
		 {
			 zkAddr = args[0];
			 cluster_name = args[1];
			 resource_name = args[2];
			 num_partition = Integer.parseInt(args[3]);
			 num_replicas = Integer.parseInt(args[4]);
		 }
		 
		 ControllerConstants cc = new ControllerConstants();
		 
		 Coordinator co = new Coordinator(zkAddr,cluster_name, cc);
		 co.startZookeeper();
		 co.setup();
		 co.addResource(resource_name, num_partition,num_replicas);
		 co.startController();
		// co.printState("State after starting the coordinator: ", resource_name);
		 
		 co.addNodeToResource(resource_name, "localhost", 30001);
		 co.addNodeToResource(resource_name, "localhost", 30002);
		 
		 co.addNodeToResource(resource_name, "localhost", 30003);
		 
		 //Thread.sleep(10000);
		 while(true)
		 {
			 Thread.sleep(10000);
			 co.printState("State after adding the 3 nodes: ", resource_name);
		 }
		 
		 //Thread.sleep(10000);
		 
	 
		// Thread.sleep(5000);
		// co.printState("State after rebalancing: ", resource_name);
		    
	 }
	  
	 
	 
	
}
