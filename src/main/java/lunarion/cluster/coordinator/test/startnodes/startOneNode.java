
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
package lunarion.cluster.coordinator.test.startnodes;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lunarion.node.LunarNode;

public class startOneNode {
	public static  ExecutorService thread_executor = Executors.newFixedThreadPool(5); 
	
	public static void main(String[] args) {
	    
		  /*
		   * use the zookeeper and Helix admin in StartCoordinator
		   */
		String zkAddr = "localhost:2199";
		String data_root = "/home/feiben";
		String cluster_name = "DBCluster"; 
		String resource_name = "RTSeventhDB";
		 
		String node_ip = "localhost";
		 
		String creation_conf = "/home/feiben/EclipseWorkspace/lunarbase-node/conf-datanode/creation.conf";
		
		int node1_port = 30001 ; 
		 
		 
		LunarNode ln1 = new LunarNode(node_ip, node1_port, zkAddr, data_root, cluster_name, resource_name, creation_conf);
		
		TaskAddNode tan1 = new TaskAddNode(ln1);
		startOneNode.thread_executor.submit(tan1);
		
		 
	  
	}

}
