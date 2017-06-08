
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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lunarion.node.LunarNode;

public class startOneNode {
	public static  ExecutorService thread_executor = Executors.newFixedThreadPool(5); 
	
	public static void main(String[] args) throws IOException {
	    
		Properties prop1 = new Properties();     
		
		if (args.length < 1) {
			System.err.println("[USAGE]: start data node configure_file");
			System.err.println("[EXAMPLE]: start ./conf-datanode/coordinator.conf");
			System.err.println("[TRY]: try to find the configure file at ./conf-datanode/");
			try {
				//InputStream in = new BufferedInputStream (new FileInputStream("/home/lunarbase/TestSpace/Controller/db_message.properties"));
				InputStream in = new BufferedInputStream (new FileInputStream("./conf-datanode/datanode.conf"));
				prop1.load(in);    
				in.close();
			}catch(Exception e){
				System.err.println("[DATA NODE ERROR]: fail to open data node configure file, system can not start up.");
				System.out.println(e);
				return;
			} 
		}
		else
		{
			String con_file = args[0];
			InputStream in = new BufferedInputStream (new FileInputStream(con_file));
			prop1.load(in);    
			in.close(); 
		}
		
		/*
		 * use the zookeeper and Helix admin in StartCoordinator
		 */
		String zkAddr = prop1.getProperty("ZOOKEEPER").trim();//localhost:2199
		String data_root = prop1.getProperty("DATA_ROOT").trim();// "/home/feiben";
		String cluster_name = prop1.getProperty("CLUSTER_NAME").trim();// "DBCluster"; 
		String resource_name  =  prop1.getProperty("RESOURCE_NAME").trim();//RTSeventhDB
		String node_ip_and_port = prop1.getProperty("NODE_IP");//data node address with port, 127.0.0.1:30001
		 
		String node_ip = node_ip_and_port.trim().split(":")[0];//"localhost";
		int node_port = Integer.parseInt(node_ip_and_port.trim().split(":")[1]);//30001 ; 
		 
		String creation_conf = prop1.getProperty("CREATION_FILE").trim();//  "/home/feiben/EclipseWorkspace/lunarbase-node/conf-datanode/creation.conf";
		
		
		  
		 
		LunarNode ln1 = new LunarNode(node_ip, node_port, zkAddr, data_root, cluster_name, resource_name, creation_conf);
		
		TaskAddNode tan1 = new TaskAddNode(ln1);
		startOneNode.thread_executor.submit(tan1);
		
		 
	  
	}

}
