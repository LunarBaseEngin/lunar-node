
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
package lunarion.cluster.coordinator.server;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import lunarion.cluster.coordinator.Resource;

public class start {

	public static void main(String[] args) throws InterruptedException, IOException, SQLException {
	
		Properties prop1 = new Properties();     
		
		if (args.length < 1) {
			System.err.println("[USAGE]: start coordinator_configure_file");
			System.err.println("[EXAMPLE]: start ./conf-coordinator/coordinator.conf");
			System.err.println("[TRY]: try to find the configure file at ./conf-coordinator/");
			try {
				//InputStream in = new BufferedInputStream (new FileInputStream("/home/lunarbase/TestSpace/Controller/db_message.properties"));
				InputStream in = new BufferedInputStream (new FileInputStream("./conf-coordinator/coordinator.conf"));
				prop1.load(in);    
				in.close();
			}catch(Exception e){
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
		
		String zkAddr	=	prop1.getProperty("ZOOKEEPER").trim();//localhost:2199
		String cluster_name  =  prop1.getProperty("CLUSTER_NAME").trim();//DBCluster
		String resource_name  =  prop1.getProperty("RESOURCE_NAME").trim();//RTSeventhDB
		int num_partition = Integer.parseInt(prop1.getProperty("PARTITION_NUM").trim());//6
		int num_replicas = Integer.parseInt(prop1.getProperty("REPLICAS_NUM").trim());//2
		String data_nodes_ips = prop1.getProperty("DATA_NODES").trim();//data nodes addresses, can be localhost or an ip
		int max_rec_per_partition = Integer.parseInt(prop1.getProperty("MAX_REC_PER_PARITION").trim());
		String meta_file = prop1.getProperty("METADATA_FILE").trim();
		String model_file = prop1.getProperty("MODEL_FILE").trim();
		
		int coordinator_port = Integer.parseInt(prop1.getProperty("COORDINATOR_PORT").trim());
		
		CoordinatorServer c_s = CoordinatorServer.getInstance();
		c_s.startServer(zkAddr, cluster_name, resource_name, num_partition, num_replicas, max_rec_per_partition, meta_file, model_file);
		
		
		String[] nodes = data_nodes_ips.split(",");
		for(int i=0;i<nodes.length;i++)
		{
			String[] node_ip_port = nodes[i].trim().split(":");
			c_s.addNodeToResource(resource_name, node_ip_port[0].trim(), Integer.parseInt(node_ip_port[1].trim()) );
		}
		
		c_s.updateMasters(resource_name);
		 
	     
		Thread.sleep(10000);
	    
		c_s.printState("State after adding the " + nodes.length + " nodes: ", resource_name);
		
		c_s.bind(coordinator_port);
		
		//System.out.println("[INFO]: coordinator started listening the port:" + coordinator_port);
			
	}
}
