
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
package lunarion.cluster.coordinator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.HelixAdmin;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView; 
import org.apache.helix.model.StateModelDefinition;

import lunarion.node.utile.ControllerConstants;
import lunarion.node.utile.Screen; 

/*
 * CLUSTER STATE: for one resource, if we have 3 nodes, 
 * and 6 partitions: 
			localhost_12000	localhost_12001	localhost_12002	
DataPartition_0			S			M			S		
DataPartition_1			S			S			M		
DataPartition_2			M			S			S		
DataPartition_3			S			S			M		
DataPartition_4			M			S			S		
DataPartition_5			S			M			S		
 * 
 * All the partitions together calls a resource.
 */
public class Coordinator {
	
	/*
	 * by default.
	 */
	private static String ZK_ADDRESS = "localhost:2199";
	private static String CLUSTER_NAME = "LunarDB_RTSeventhDB";
	
	
	/* actually it is a master-slave model*/
	private static final String STATE_MODEL_NAME = ControllerConstants.STATE_MODEL_NAME;
	
	// states
	private static final String SLAVE = "SLAVE";
	private static final String OFFLINE = "OFFLINE";
	private static final String MASTER = "MASTER";
	private static final String DROPPED = "DROPPED";
	
	//private ResourceManager resource_manager = new ResourceManager();
	/*
	 * <resource_name, Resource>
	 */
	private HashMap<String, Resource> resource_list = new HashMap<String, Resource>(); 
	
	//private static List<LunarNode> NODE_LIST;
	private static HelixAdmin admin;
	private ControllerConstants controller;
	
	public Coordinator(String _zookeeper_addr, String _cluster_name, ControllerConstants _c_p)
	{
		ZK_ADDRESS = _zookeeper_addr;
		CLUSTER_NAME = _cluster_name;
		controller = _c_p;
	}
	
	public void shutdown()
	{
		Iterator<String> resources = resource_list.keySet().iterator();
		while(resources.hasNext())
		{
			Resource res  = resource_list.get(resources.next());
			res.close();
		}
	}
	public void startZookeeper( ) 
	{
		Screen.echo("STARTING Zookeeper at: " + ZK_ADDRESS);
	    IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {
	      @Override
	      public void createDefaultNameSpace(ZkClient zkClient) {
	      }
	    };
	    new File("/tmp/lunar-coordinator-start").mkdirs();
	    // start zookeeper
	    ZkServer server =  
	    		new ZkServer("/tmp/lunar-coordinator-start/dataDir", 
	    					"/tmp/lunar-coordinator-start/logDir",
	    					defaultNameSpace, 2199);
	    server.start();  
	}
	
	public static void setup() 
	{
	    admin = new ZKHelixAdmin(ZK_ADDRESS);
	    // create cluster
	    Screen.echo("Creating cluster: " + CLUSTER_NAME);  
	    admin.addCluster(CLUSTER_NAME, true);

	    // Add nodes to the cluster
	    /*
	    Screen.echo("Adding " + NUM_NODES + " participants to the cluster");
	    for (int i = 0; i < NUM_NODES; i++) {
	      admin.addInstance(CLUSTER_NAME, INSTANCE_CONFIG_LIST.get(i));  
	      Screen.echo("\t Added participant: " + INSTANCE_CONFIG_LIST.get(i).getInstanceName());
	    }
	    */

	    // Add a state model
	    StateModelDefinition myStateModel = defineStateModel();  
	    Screen.echo("Configuring StateModel: " + STATE_MODEL_NAME +" with 1 Master and 1 Slave");
	    admin.addStateModelDef(CLUSTER_NAME, STATE_MODEL_NAME, myStateModel);

	   
	}
	
	public void addResource( String _resource_name, int _num_partitions, int _num_replicas)
	{
		Resource res = new Resource( admin, 
				CLUSTER_NAME, 
				_resource_name, 
				_num_partitions, 
				_num_replicas);

		resource_list.put(_resource_name, res);	
		
		 // Add a resource with 6 partitions and 2 replicas
	    Screen.echo("Adding a resurce "+_resource_name +": " + "with "+_num_partitions+" partitions and "+_num_replicas +" replicas");
	    admin.addResource(CLUSTER_NAME, _resource_name, _num_partitions, STATE_MODEL_NAME, "AUTO");
	    // this will set up the ideal state, it calculates the preference list for
	    // each partition similar to consistent hashing
	    //admin.rebalance(CLUSTER_NAME, _resource_name, _num_replicas);  
	}

	public Resource getResource(String res_name)
	{
		return resource_list.get(res_name);
	}
	private static StateModelDefinition defineStateModel() 
	{
	    StateModelDefinition.Builder builder = new StateModelDefinition.Builder(STATE_MODEL_NAME);
	    // Add states and their rank to indicate priority. Lower the rank higher the
	    // priority
	    builder.addState(MASTER, 1);
	    builder.addState(SLAVE, 2);
	    builder.addState(OFFLINE);
	    builder.addState(DROPPED);
	    // Set the initial state when the node starts
	    builder.initialState(OFFLINE);

	    // Add transitions between the states.
	    builder.addTransition(OFFLINE, SLAVE);
	    builder.addTransition(SLAVE, OFFLINE);
	    builder.addTransition(SLAVE, MASTER);
	    builder.addTransition(MASTER, SLAVE);
	    builder.addTransition(OFFLINE, DROPPED);

	    // set constraints on states.
	    // static constraint
	    builder.upperBound(MASTER, 1);
	    // dynamic constraint, R means it should be derived based on the replication
	    // factor.
	    builder.dynamicUpperBound(SLAVE, "R");

	    StateModelDefinition statemodelDefinition = builder.build();
	    return statemodelDefinition;
	  
	}
	
	public static void startController() 
	{ 
	    // start controller
		Screen.echo("Starting Helix Controller");
	    HelixControllerMain.startHelixController(ZK_ADDRESS, CLUSTER_NAME, "localhost_9100",
	        HelixControllerMain.STANDALONE); 
	}
	
	public boolean addNodeToResource(String resource_name, String node_ip, int node_port) throws Exception 
	{ 
		 if(this.resource_list.get(resource_name)!=null)
		 {
			 this.resource_list.get(resource_name).addNode(node_ip, node_port);
			 return true;
		 }
		 else
			 return false;
	} 
	
	public void stopNodeInResource(String resource_name)
	{
		
	}
	public boolean rebalanceInResource(String resource_name)
	{
		Resource res = this.resource_list.get(resource_name);
		if(res != null)
		{ 
			res.rebalance();
			return true; 
		} 
		
		return false;
	}
	
	  
	 public void printState(String msg, String resource_name) 
	 {
		 System.out.println("CLUSTER "+CLUSTER_NAME+" STATE: " + msg);
		 ExternalView resourceExternalView = admin.getResourceExternalView(CLUSTER_NAME, resource_name);
		 
		 TreeSet<String> sortedSet = new TreeSet<String>(resourceExternalView.getPartitionSet());
		 StringBuilder sb = new StringBuilder("\t\t");
		 
		 Resource res = this.resource_list.get(resource_name);
				 
		 if(res == null)
		 {
			 System.err.println("CLUSTER does not has resource " + resource_name);	 
			 return;
		 }
		 
		 for (int i = 0; i < res.getNodeNumber(); i++) 
		 {
		      sb.append(res.getInstantConfig(i).getInstanceName()).append("\t"); 
		 }
		 System.out.println(sb);
		 for (String partitionName : sortedSet) 
		 {
		      sb.delete(0, sb.length() - 1);
		      sb.append(partitionName).append("\t");
		      for (int i = 0; i < res.getNodeNumber(); i++) 
		      { 
		    	  Map<String, String> stateMap = resourceExternalView.getStateMap(partitionName);
		    	  if (stateMap != null && stateMap.containsKey(res.getInstantConfig(i).getInstanceName())) {
		    		  sb.append(stateMap.get(res.getInstantConfig(i).getInstanceName()).charAt(0)).append(
		    				  "\t\t");
		    	  } 
		    	  else 
		    	  {
		    		  sb.append("-").append("\t\t");
		    	  }
		      } 
		      System.out.println(sb); 
		 }  
		 System.out.println("============================================================"); 
	 }

}
