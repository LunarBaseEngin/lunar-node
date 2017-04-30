
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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
import lunarion.node.EDF.NodeTaskCenter;
import lunarion.node.EDF.events.VNodeIncomingRecords;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseQuery;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.utile.ControllerConstants;

public class TaskReplication implements Runnable {

    private String master_addr; 
    private int db_port;
    private Logger replicator_logger = null;
    private final LunarDBClient client_to_master ;
   // private final LunarDB local_db;
    private LunarDBServerStandAlone db_server; 
    
    private final String partition_name;
    private final String resource_name;
    
    private AtomicBoolean shutdown_requested = new AtomicBoolean(false);
    //private Thread r_thread;
    
    TaskReplication( LunarDBServerStandAlone  _db_server, LunarDBClient _client_to_master, String _master_addr, int _db_port, 
    				Logger _partition_logger,
    				String _partition_name,
    				String _resource_name ) 
    {
        //this.local_db = _local_db;
    	
    	this.db_server = _db_server;
        this.master_addr = _master_addr;
        this.replicator_logger =  _partition_logger;
        this.db_port = _db_port; 
        this.partition_name = _partition_name;
        this.resource_name = _resource_name;
        this.client_to_master = _client_to_master;
    }

    public void run() { 
    	
    	
    	if(client_to_master.isConnected())
 		{
    		System.out.println("@TaskReplication.run(), Master of partition: "+ partition_name + " is connected.");
			replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @TaskReplication.run(), Master of partition: "+ partition_name + " is connected.");
	 
	    	while (!shutdown_requested.get())
	    	{
	    		try {
					replicateFromMaster( ) ;
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    	}  
 		} 
		else
		{
			System.out.println("@TaskReplication.run(), Master of partition: "+ partition_name + " is not connected.");
			replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @TaskReplication.run(), Master of partition: "+ partition_name + " is not connected.");
		} 
    	
        
    }
    
    private void replicateFromMaster( ) throws InterruptedException
	{ 
 			System.out.println(" @TaskReplication.replicateFromMaster(), start replicating data of partition: " + partition_name + " from master: " + this.master_addr + "_"+db_port);   
 	 		
 			int partition_number = ControllerConstants.parsePartitionNumber(partition_name);
 			
 			CMDEnumeration.command cmd = CMDEnumeration.command.fetchTableNamesWithSuffix;
        	String[] params = new String[2];
        	params[0] = resource_name;  
        	params[1] = ControllerConstants.patchPartitionLogSuffix( partition_number);
        	
        	MessageResponse resp_from_svr = client_to_master.sendRequest(cmd, params); 
        	 	
 			if(resp_from_svr != null && resp_from_svr.isSucceed())
 			{
 				/*
 				System.out.println("LunarNode responded command: "+ resp_from_svr.getCMD());
 	    		System.out.println("LunarNode responded UUID: "+ resp_from_svr.getUUID());
 	    		System.out.println("LunarNode responded suceed: "+ resp_from_svr.isSucceed());
 	    		*/
 	    		for(int i=0;i<resp_from_svr.getParams().length;i++)
 	    		{
 	    			System.out.println("LunarNode responded: "+ resp_from_svr.getParams()[i]);
 	    			
 	    			String table = resp_from_svr.getParams()[i];
 	    			
 	    			
 	    			
 	    		}
 			}
 			else
 			{
 				System.out.println(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), no table for partition: " + partition_name + " of resource " + resource_name + " exist.");
 				replicator_logger.info(Timer.currentTime()+ " [NODE INFO]: @replicateFromMaster(), no table for partition: " + partition_name + " of resource " + resource_name + " exist.");
	
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
