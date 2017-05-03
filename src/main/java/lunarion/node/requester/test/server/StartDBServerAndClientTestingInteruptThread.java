
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
package lunarion.node.requester.test.server;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.LunarNode;
import lunarion.node.logger.LoggerFactory;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.requester.test.TestMultipleClient;
import lunarion.node.requester.test.TestMultipleClient.TaskSendCMD;

public class StartDBServerAndClientTestingInteruptThread {
	
	public static  ExecutorService thread_executor = Executors.newFixedThreadPool(6); 
	public static  ExecutorService thread_server = Executors.newFixedThreadPool(1); 
	
	
	public static void main(String[] args) throws Exception {
        int port = 9090;
        if (args != null && args.length > 0) {
            try {
                port = Integer.valueOf(args[0]);
            } catch (NumberFormatException e) {

            }
        } 
        StartDBServerAndClientTestingInteruptThread sdbactit = new StartDBServerAndClientTestingInteruptThread();
        
        TaskStartServer tss = sdbactit.getServerTask();
        thread_server.submit(tss);
        
        CMDEnumeration.command cmd = CMDEnumeration.command.fetchTableNamesWithSuffix;
    	String[] params = new String[2];
    	params[0] = "CorpusDB"; 
    	params[1] = "_1";  
		
    	TestMultipleClient tmc= new TestMultipleClient(); 
        
        for(int i=0;i<20;i++)
        {
        	LunarDBClient clienti = new LunarDBClient();
       	 	clienti.connect("127.0.0.1", port);
       	 	TaskSendCMD ti = tmc.getTask(clienti, cmd, params);
            
            thread_executor.submit(ti);
        }
    }
	
	public TaskStartServer getServerTask( )
	{
		return new TaskStartServer() ; 
	}
	
	
	public class TaskStartServer implements Runnable {

		int port = 9090;
		 String svr_root = "/home/feiben/DBTest/LunarNode/";
		 
		 LunarDBServerStandAlone lsa = new LunarDBServerStandAlone();
	        
	    
		 TaskStartServer( ) {
	         
		 }

	    public void run() {  
	       
	        try {
	        	lsa.startServer(svr_root, LoggerFactory.getLogger("LunarNode")); 
		        
	        	lsa.bind(port);
	        } 
	        catch(InterruptedException|IOException e)
	        {
	        	 e.printStackTrace();
	        } 
	        finally {
	        	lsa.closeServer(); 
	        }
	    } 
	}


	
}
 
