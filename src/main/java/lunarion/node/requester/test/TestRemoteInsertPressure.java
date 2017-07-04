
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import LCG.RecordTable.StoreUtile.Record32KBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;

public class TestRemoteInsertPressure {
	public static void main(String[] args) throws Exception {
        int port = 9090;
        if (args != null && args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (Exception e) {
            }
        }
        LunarDBClient client = new LunarDBClient();
        client.connect("127.0.0.1", port);
        String column = "content";
        try
        { 
        	ChannelFuture cf = client.channel_list.get(0);
         
        
        
        	String corpus_file = "/home/feiben/EclipseWorkspace/lunarbase-node/corpus/Test_20000line.txt";
        	long duration = 0; 
        	int total_recs = 0;
        	for(int jj = 0;jj<50;jj++)
			{
			
			try (BufferedReader br =
					 new BufferedReader(new FileReader(corpus_file))) {
				 int line_count = 0;
				 int iter = 1000;	
				 int count = 0;
				
				 String[] recs = new String[iter];
				 for (String line = br.readLine(); line != null  /*&& line_count<= 100000*/  ; line = br.readLine())
				 {
					 if(count < iter)
					 {
						 /*
						  * a double char comma, since it is a Chinese corpus
						  */
						 String[] per_line = line.split("，");
						 for(int i=0;i<per_line.length && count<iter;i++)
						 {
							 recs[count] = "{" + column +"=[\"" + per_line[i] + "\"]}"; 
							 count++;
						 } 
					 }
					 else
					 {
						 total_recs += count;
						 CMDEnumeration.command cmd = CMDEnumeration.command.insert;
						 recs[0] = "CorpusDB";
						 recs[1] = "textTable_remote_1";
			        		
						 long start_time = System.nanoTime();  
						 RemoteResult resp_from_svr = client.sendRequest(cmd, recs); 
			           	 
						 long end_time = System.nanoTime();
						 if(resp_from_svr == null)
							 System.err.println("table may be closed or removed, fail to insert."); 
						 else
						 {
							 long duration_each = end_time - start_time;
							 duration += duration_each;
							 count = 0;
							 recs = new String[iter];
							 System.out.println("Line " + line_count + " loaded."); 
							 System.out.println("insert  " + iter + " lines costs: " + duration_each/1000000 + " ms"); 
							 System.out.println("LunarNode responded command: "+ resp_from_svr.getCMD());
							 System.out.println("LunarNode responded UUID: "+ resp_from_svr.getUUID());
							 System.out.println("LunarNode responded suceed: "+ resp_from_svr.isSucceed());
							 for(int j=0;j<resp_from_svr.getParams().length;j++)
							 {
								 System.out.println("LunarNode responded: "+ resp_from_svr.getParams()[j]);
							 }
						 }
						
					 }
					 line_count ++; 

				 }
				 
				 
			 } catch (IOException ioe) {
				 ioe.printStackTrace();
			 }
			}
        	System.out.println("insert  " + total_recs + " costs: " + duration/1000000 + " ms"); 

        	/*
        	 * it's ok, use condition.await(10*1000, TimeUnit.MILLISECONDS)
        	 * at MessageClientWatcher 
        	 * to block the thread waiting for server response.
        	 */
        	// cf.channel().closeFuture().sync();
        
        } finally {
            
        	client.shutdown();
        }
        
    }
}
