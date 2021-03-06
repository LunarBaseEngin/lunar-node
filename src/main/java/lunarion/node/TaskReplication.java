
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
package lunarion.node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

import LCG.DB.API.LunarDB;
import LCG.DB.API.Result.FTQueryResult;
import LCG.EnginEvent.Interfaces.LFuture;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lunarion.db.local.shell.CMDEnumeration; 
import lunarion.node.logger.LogCMDConstructor;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForQuery;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

public class TaskReplication implements Runnable {

    private String master_addr; 
    private int master_db_port;
    private Logger replicator_logger = null;
    private final LunarDBClient client_to_master ;
   // private final LunarDB local_db;
    
    private LunarDBServerStandAlone db_server; 
    
    private final String partition_name;
    private final String resource_name;
    private final String instance_name;
    
    private AtomicBoolean shutdown_requested = new AtomicBoolean(false);
	BlockingQueue<String[]> update_notification_queue = null;
    //private String[] update_db_and_table;
    
    //private Thread r_thread;
    
    TaskReplication( String _instance_name,
    				LunarDBServerStandAlone  _db_server, 
    				LunarDBClient _client_to_master,
    				String _master_addr, 
    				int _db_port, 
    				Logger _partition_logger,
    				String _partition_name,
    				String _resource_name,
    				BlockingQueue<String[]> _update_notification_queue) 
    {  
    	this.instance_name = _instance_name;
    	this.db_server = _db_server;
        this.master_addr = _master_addr;
        this.replicator_logger =  _partition_logger;
        this.master_db_port = _db_port; 
        this.partition_name = _partition_name;
        this.resource_name = _resource_name;
        this.client_to_master = _client_to_master;
        
        this.update_notification_queue = _update_notification_queue;
        //this.update_db_and_table = _update_db_and_table;
    }

    public void run() { 
    	
    	try{
    			int failure_count = 0 ;
    			String[] db_and_table_to_be_updated = null; 
		    	while (!shutdown_requested.get() )
		    	{
		    		if(client_to_master.isConnected())
		    		{
			    		try { 
			    			System.err.println("waiting for message........");
			    			db_and_table_to_be_updated=update_notification_queue.poll(3, TimeUnit.SECONDS) ;
			    			if(db_and_table_to_be_updated != null)
			    			{
			    				System.err.println("get........");			    			
			    				replicateFromMaster(db_and_table_to_be_updated) ;
			    			}
			    			
			    			//replicateFromMaster( ) ; 
							
							Thread.sleep(5000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
		    		}
		    		else
		    		{
		    			failure_count ++;
		    			if(failure_count >=10)
		    			{
		    				shutdown_requested.set(true);
		    			}
		    			else
		    			{
		    				try {
								Thread.sleep(5000);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
		    				System.out.println("@TaskReplication.run(), Master of partition: "+ partition_name + " is not connected.");
		    				replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @TaskReplication.run(), Master of partition: "+ partition_name + " is not connected.");
		    			}
		    		}
		    	}
		    	//client_to_master.shutdown();
		    	 
		    	 
		    	System.err.println("@TaskReplication.run() has been shutdown");
		    	replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @TaskReplication.run() has been shutdown");
		 
    	}
    	finally
    	{
    		System.err.println("@TaskReplication.run() has been interrupted");
			
    		replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @TaskReplication.run() has been interrupted");
    		client_to_master.shutdown();
    	} 
    }
    
    public void shutdown()
    {
    	this.shutdown_requested.set(true);
    	
    } 
    
    private boolean checkPartition(String name)
    {
    	int partition_number = ControllerConstants.parsePartitionNumber(partition_name);
    	int partition_to_be_checked = ControllerConstants.parsePartitionNumber(name);
    	if(partition_number != partition_to_be_checked)
    		return false;
    	
    	return true;
			
    }
    
    private void executeLog(RemoteResult resp_of_logs )
    {
    	for(int j=0; j<resp_of_logs.getParams().length;j++)
		{
			System.out.println("LunarNode responded: "+ resp_of_logs.getParams()[j]);
			MessageRequest logged_cmd = LogCMDConstructor.parseLoggedCMD(resp_of_logs.getParams()[j]);
			if(logged_cmd != null )
			{
				TaskHandlingMessage replication_task 
													= new TaskHandlingMessage(logged_cmd, 
																				db_server,  
																				replicator_logger);
				replication_task.run();
				System.out.println("command: "+ logged_cmd.getCMD() + " executed succeed.");
				replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), " + "command: "+ logged_cmd.getCMD() + " executed succeed.");			
			}
			else
			{
				System.err.println("logged_cmd is null, no command can be executed." );
				replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), logged_cmd is null, no command can be executed. ");
    				
			}
		}  
    }
    private void replicateFromMaster(String[] db_and_table) throws InterruptedException
    {
    	if(db_and_table==null || db_and_table.length<2)
		{
			return;
		}
    	
    	System.out.println(" @TaskReplication.replicateFromMaster(...), start replicating data of partition: " + partition_name + " from master: " + this.master_addr + "_"+master_db_port);   
    	replicator_logger.info(Timer.currentTime() 
							+ " [NODE INFO]:  @TaskReplication.replicateFromMaster(...), start replicating data of partition: " 
							+ partition_name 
							+ " from master: " 
							+ this.master_addr 
							+ "_"
							+ master_db_port);
		
		
    	String table_name = db_and_table[1];
		if(!checkPartition(table_name))
		{		
			replicator_logger.info(Timer.currentTime() 
								+ " [NODE ERROR]:  @TaskReplication.replicateFromMaster(...), wrong table to be replicated: " 
								+ table_name );
		}
		
		String log_table_name =  ControllerConstants.getLogTableName(table_name);
		CMDEnumeration.command get_logs = CMDEnumeration.command.fetchRecordsASC;
		String[] params_for_log = new String[4];
		params_for_log[0] = resource_name; 
		params_for_log[1] = log_table_name; 
		params_for_log[2] = "0";
		params_for_log[3] = "15";
		RemoteResult resp_of_logs = client_to_master.sendRequest(get_logs, params_for_log); 
 	 	
		if(resp_of_logs != null && resp_of_logs.isSucceed())
		{ 
			executeLog( resp_of_logs );
		}
		else
		{
			System.err.println("log table is emtpy as of now." );
			replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @TaskReplication.replicateFromMaster(...), log table is emtpy as of now.");	
		}	
    }
    private void replicateFromMaster() throws InterruptedException
	{ 
 			System.out.println(" @TaskReplication.replicateFromMaster(), start replicating data of partition: " + partition_name + " from master: " + this.master_addr + "_"+master_db_port);   
 			replicator_logger.info(Timer.currentTime() 
 							+ " [NODE INFO]:  @TaskReplication.replicateFromMaster(), start replicating data of partition: " 
 							+ partition_name 
 							+ " from master: " 
 							+ this.master_addr 
 							+ "_"
 							+ master_db_port);
				
 			int partition_number = ControllerConstants.parsePartitionNumber(partition_name);
 			
 			CMDEnumeration.command cmd = CMDEnumeration.command.fetchTableNamesWithSuffix;
        	String[] params = new String[2];
        	params[0] = resource_name;  
        	params[1] = ControllerConstants.patchPartitionLogSuffix( partition_number);
        	replicator_logger.debug(Timer.currentTime()+ " [NODE INFO]:  @TaskReplication.replicateFromMaster(), sending");
			
        	RemoteResult resp_from_svr = client_to_master.sendRequest(cmd, params); 
        	 	
 			if(resp_from_svr != null && resp_from_svr.isSucceed())
 			{
 				replicator_logger.debug(Timer.currentTime()+ " [NODE INFO]:  @TaskReplication.replicateFromMaster(), receieved");
 				
 				/*
 				System.out.println("LunarNode responded command: "+ resp_from_svr.getCMD());
 	    		System.out.println("LunarNode responded UUID: "+ resp_from_svr.getUUID());
 	    		System.out.println("LunarNode responded succeed: "+ resp_from_svr.isSucceed());
 	    		*/
 	    		for(int i=0; i<resp_from_svr.getParams().length;i++)
 	    		{
 	    			System.out.println("LunarNode responded: "+ resp_from_svr.getParams()[i]);
 	    			
 	    			String table = resp_from_svr.getParams()[i];
 	    			CMDEnumeration.command get_logs = CMDEnumeration.command.fetchRecordsASC;
 	    			String[] params_for_log = new String[4];
 	    			params_for_log[0] = resource_name; 
 	    			params_for_log[1] = table; 
 	    			params_for_log[2] = "0";
 	    			params_for_log[3] = "15";
 	    			
 	    			RemoteResult resp_of_logs = client_to_master.sendRequest(get_logs, params_for_log); 
 	        	 	
 	    			if(resp_of_logs != null && resp_of_logs.isSucceed())
 	    			{
 	    				/*
 	    				System.out.println("LunarNode responded command: "+ resp_of_logs.getCMD());
 	    	    		System.out.println("LunarNode responded UUID: "+ resp_of_logs.getUUID());
 	    	    		System.out.println("LunarNode responded succeed: "+ resp_of_logs.isSucceed());
 	    	    		 
 	    	    		for(int j=0; j<resp_of_logs.getParams().length;j++)
 	    	    		{
 	    	    			System.out.println("LunarNode responded: "+ resp_of_logs.getParams()[i]);
 	    	    			
 	    	    		}*/
 	    				
 	    				 
 	    				for(int j=0; j<resp_of_logs.getParams().length;j++)
 	    	    		{
 	    					
 	    	    			System.out.println("LunarNode responded: "+ resp_of_logs.getParams()[j]);
 	    	    			MessageRequest logged_cmd = LogCMDConstructor.parseLoggedCMD(resp_of_logs.getParams()[j]);
 	    	    			if(logged_cmd != null )
 	    	    			{
 	    	    				TaskHandlingMessage replication_task 
 																= new TaskHandlingMessage(logged_cmd, 
 																							db_server,  
 																							replicator_logger);
 	    	    				replication_task.run();
 	    	    				System.out.println("command: "+ logged_cmd.getCMD() + " executed succeed.");
 	    	    				replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), " + "command: "+ logged_cmd.getCMD() + " executed succeed.");
 	    	    				
 	    	    			}
 	    	    			else
 	    	    			{
 	    	    				System.err.println("logged_cmd is null, no command can be executed." );
 	    	    				replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), logged_cmd is null, no command can be executed. ");
 	    	    				
 	    	    			}
 	    	    		}  
 	    			}
 	    			else
 	    			{
 	    				System.err.println("log table is emtpy as of now." );
 	    				replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), log table is emtpy as of now.");
 	    				
 	    			}
 	    		}
 			}
 			else
 			{
 				System.out.println(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), no table for partition: " + partition_name + " of resource " + resource_name + " on master: " + this.master_addr + "_"+this.master_db_port+ " exist.");
 				replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), no table for partition: " + partition_name + " of resource " + resource_name + " at " + this.master_addr + "_"+this.master_db_port+ " exist.");
	
 			}
 		
	}
    
    /*
     * will not use this method, since the lunarDBClient sends signal to interrupt the client thread, 
     * which is waiting for the server response, 
     * hence this thread will be interrupted at the same time, since the it is the same thread 
     * where the client is, and MessageResponse will not have value.
     */
    /*
    public void startRep()
    {
    	r_thread = new Thread(this);
    	r_thread.start();
    }
    public void stopRep() {
    	shutdown_requested.set(true);
    	client.shutdown();
    	 r_thread.interrupt();
	}*/

     
}
