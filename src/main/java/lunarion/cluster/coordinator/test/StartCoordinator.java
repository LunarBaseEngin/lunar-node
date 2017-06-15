
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

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
		
		Properties prop1 = new Properties();     
		try {
			//InputStream in = new BufferedInputStream (new FileInputStream("/home/lunarbase/TestSpace/Controller/db_message.properties"));
			InputStream in = new BufferedInputStream (new FileInputStream("/home/feiben/EclipseWorkspace/lunarbase-node/conf-coordinator/coordinator.conf"));
			prop1.load(in);    
			in.close();
		}catch(Exception e){
	            System.out.println(e);
	    }
		String zkAddr	=	prop1.getProperty("ZOOKEEPER").trim();//localhost:2199
		String cluster_name  =  prop1.getProperty("CLUSTER_NAME").trim();//DBCluster
		String resource_name  =  prop1.getProperty("RESOURCE_NAME").trim();//RTSeventhDB
		int num_partition = Integer.parseInt(prop1.getProperty("PARTITION_NUM").trim());//6
		int num_replicas = Integer.parseInt(prop1.getProperty("REPLICAS_NUM").trim());//2
		String node_ip = prop1.getProperty("DATA_NODES");//data node address, can be localhost or an ip
		int max_rec_per_partition = Integer.parseInt(prop1.getProperty("MAX_REC_PER_PARITION").trim());
		String meta_file = prop1.getProperty("METADATA_FILE").trim();
		String model_file = prop1.getProperty("MODEL_FILE").trim();
		
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
		 
		Coordinator co = new Coordinator();
		co.init(zkAddr,cluster_name, cc);
		 
		co.startZookeeper();
		co.setup();
		co.addResource(resource_name, num_partition,num_replicas, max_rec_per_partition, meta_file,  model_file );
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
