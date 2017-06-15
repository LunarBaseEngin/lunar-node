/** LCG(Lunarion Consultant Group) Confidential
 * LCG LunarBase team is funded by LCG.
 * 
 * @author LunarBase team, contacts: 
 * redhat@lunarion.com
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
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import lunarion.cluster.coordinator.Coordinator;
import lunarion.cluster.coordinator.Resource;
import lunarion.cluster.coordinator.ResponseCollector;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

public class sendRequestToMaster_insert2 {

	public static void main(String[] args) throws InterruptedException, IOException {
		
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
		String node_ip =prop1.getProperty("DATA_NODES");//data node address, can be localhost or an ip
		int max_rec_per_partition = Integer.parseInt(prop1.getProperty("MAX_REC_PER_PARITION").trim());
		String meta_file = prop1.getProperty("METADATA_FILE").trim();
		String model_file = prop1.getProperty("MODEL_FILE").trim();
		
		 if (args.length < 6) {
		      System.err.println("[USAGE]: StartCoordinator _zk_ddress _cluster_name _resource_name _num_partiotions _num_replicas");
		      System.err.println("[EXAMPLE]: Start 127.0.0.1:2199 my_cluster my_resource 6 2 /home/feiben/a.properties");
		      
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
		 co.addResource(resource_name, num_partition,num_replicas, max_rec_per_partition, meta_file, model_file );
		 co.startController();
		// co.printState("State after starting the coordinator: ", resource_name);
		 
		 try
		 { 
			 String[] nodes = node_ip.split(",");
			 for(int i=0;i<nodes.length;i++)
			 {
					String[] node_ip_port = nodes[i].trim().split(":");
					co.addNodeToResource(resource_name, node_ip_port[0], Integer.parseInt(node_ip_port[1].trim()) );
			 }
		 }
		 catch(Exception e)
		 {
			 e.printStackTrace();
		 }
		 //Thread.sleep(10000);
		 Resource res = co.getResource(resource_name);
		 res.updateMasters( ) ;
		 
	     Thread.sleep(10000);
	     co.printState("State after adding the 3 nodes: ", resource_name);
		 CMDEnumeration.command cmd = CMDEnumeration.command.insert;
		 int length = 1000;
		 long time_per1000=0;
		 String[] params = new String[length];
		 params[0] = "RTSeventhDB";
		 params[1] = "node_table"; 
		 int i=2;	
		 int m=0;
		 for(int j=0; j<1; j++) {
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream("/home/feiben/EclipseWorkspace/lunarbase-node/corpus/Test_20000line.txt"), "utf-8"));
				String line = null;
				
				while ((line = br.readLine()) != null) {
					
					//System.out.println(StringFilter(line)+i);
					params[i] = "{content=[\" this is the test content " +line+".\"]}";
					i++;
					if(i%1000==0) {
						long startTime=System.currentTimeMillis(); 
						ResponseCollector rc = res.sendRequest(cmd, params);
						long endTime=System.currentTimeMillis(); 
						m++;
						rc.printResponse();
						i=2;
						long time=endTime-startTime;
						System.out.println("*"+time+"*"+"#"+m+"#");
						time_per1000=time_per1000+time;
						System.out.println(time_per1000);
						//continue;
					}
				}
				
			} catch (Exception e) {
			} finally {
				try {
					if (br != null)
						br.close();
				} catch (IOException e) {
				}
			}
		}
		
		System.out.println("程序运行时间： "+(time_per1000)+"ms"); 
	}
}
