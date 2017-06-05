
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
package lunarion.node.requester.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.netty.channel.ChannelFuture;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarNode;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;

public class TestMultipleClient {

	
	public static  ExecutorService thread_executor = Executors.newFixedThreadPool(5); 
	
	public static void main(String[] args) throws Exception {
		
		/*
		CMDEnumeration.command cmd = CMDEnumeration.command.fetchLog;
    	String[] params = new String[4];
    	params[0] = "CorpusDB"; 
    	params[1] = "textTable_remote_1"; 
    	params[2] = "0";
    	params[3] = "15"; 
         */
		 int port = 9090;
		     
		
    	CMDEnumeration.command cmd = CMDEnumeration.command.fetchTableNamesWithSuffix;
    	String[] params = new String[2];
    	params[0] = "CorpusDB"; 
    	params[1] = "_1";  
		
    	TestMultipleClient tmc= new TestMultipleClient(); 
         
        
        for(int i=0;i<25;i++)
        {
        	/*
        	 * must be noticed that the client.connect() has to be outside the thread task, 
        	 * otherwise the task will be blocked and never quit, hence the following 
        	 * tasks can not be submit to the executor service.  
        	 */
        	 LunarDBClient clienti = new LunarDBClient();
        	 clienti.connect("127.0.0.1", port);
            TaskSendCMD ti = tmc.getTask(clienti, cmd, params);
            
            thread_executor.submit(ti);
        }
        
        //thread_executor.shutdownNow();
	}
	
	public TaskSendCMD getTask(LunarDBClient client, CMDEnumeration.command _cmd, String[] _args )
	{
		return new TaskSendCMD(  client, _cmd, _args) ; 
	}
	
	public class TaskSendCMD implements Runnable {

	    private CMDEnumeration.command cmd;
	    private String[] arges = null;
	    LunarDBClient client = null;
	    int port = 9090;
	    
	    TaskSendCMD(LunarDBClient _client, CMDEnumeration.command _cmd, String[] _args ) {
	        this.cmd = _cmd; 
	        arges = _args;
	        client = _client;
	    }

	    public void run() 
	    {
	    	RemoteResult resp_from_svr = null;
	    	
	    	try {
	    		resp_from_svr = client.sendRequest(cmd, arges, 5*1000); 
	    		
	    		//Thread.sleep(10000);
	    		
	    	} catch (InterruptedException e) 
	    	{
	    		e.printStackTrace();
	    	}
	    	 			
	   	           	 
	    	System.out.println(Thread.currentThread().getId() + " LunarNode responded command: "+ resp_from_svr.getCMD());
	    	System.out.println(Thread.currentThread().getId() + " LunarNode responded UUID: "+ resp_from_svr.getUUID());
	    	System.out.println(Thread.currentThread().getId() + " LunarNode responded suceed: "+ resp_from_svr.isSucceed());
	    	for(int i=0;i<resp_from_svr.getParams().length;i++)
	    	{
	    		System.out.println(Thread.currentThread().getId() + " LunarNode responded: "+ resp_from_svr.getParams()[i]);
	    	}
	    	 		 
	    	System.out.println(Thread.currentThread().getId() + " shutdown the client ");
		        		
	    	client.shutdown();
		        		
	    	System.out.println(Thread.currentThread().getId() + " shutdown ok "); 
			 
	    	 
	    	//System.out.println(Thread.currentThread().getId() + " shutdown ok ");
	        
	    }
	}
}


