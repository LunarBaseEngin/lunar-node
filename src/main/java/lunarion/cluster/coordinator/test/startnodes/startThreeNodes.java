
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

public class startThreeNodes {
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
		 
		String creation_conf = "/home/feiben/EclipseWorkspace/LunarBaseApplication/creation.conf";
		
		int node1_port = 30001 ; 
		
		/*
		String[] node1_arges = new String[7];
		node1_arges[0] = zkAddr;
		node1_arges[1] = data_root;
		node1_arges[2] = cluster_name;
		node1_arges[3] = resource_name;
		node1_arges[4] = node_ip;
		node1_arges[5] = ""+node1_port;
		node1_arges[6] = creation_conf;
		LunarNode.main(node1_arges);
		*/
		LunarNode ln1 = new LunarNode(node_ip, node1_port, zkAddr, data_root, cluster_name, resource_name, creation_conf);
		
		TaskAddNode tan1 = new TaskAddNode(ln1);
		startThreeNodes.thread_executor.submit(tan1);
		
		int node2_port = 30002 ; 
		LunarNode ln2 = new LunarNode(node_ip, node2_port, zkAddr, data_root, cluster_name, resource_name, creation_conf);
		TaskAddNode tan2 = new TaskAddNode(ln2);
		startThreeNodes.thread_executor.submit(tan2);
		
		
		int node3_port = 30003 ; 
		LunarNode ln3 = new LunarNode(node_ip, node3_port, zkAddr, data_root, cluster_name, resource_name, creation_conf);
		TaskAddNode tan3 = new TaskAddNode(ln3);
		startThreeNodes.thread_executor.submit(tan3);
		
		
		/*
		int node2_port = 30002 ; 
		String[] node2_arges = new String[7];
		node2_arges[0] = zkAddr;
		node2_arges[1] = data_root;
		node2_arges[2] = cluster_name;
		node2_arges[3] = resource_name;
		node2_arges[4] = node_ip;
		node2_arges[5] = ""+node2_port;
		node2_arges[6] = creation_conf;
		
		LunarNode.main(node2_arges);
		
		
		String node3_port = "30003"; 
		String[] node3_arges = new String[7];
		node3_arges[0] = zkAddr;
		node3_arges[1] = data_root;
		node3_arges[2] = cluster_name;
		node3_arges[3] = resource_name;
		node3_arges[4] = node_ip;
		node3_arges[5] = node3_port;
		node3_arges[6] = creation_conf;
	
		LunarNode.main(node3_arges);
		*/
	}

}
