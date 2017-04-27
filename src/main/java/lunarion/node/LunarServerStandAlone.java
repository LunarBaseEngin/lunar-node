
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor; 
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.SimpleLayout;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine;
import LCG.FSystem.Def.DBFSProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lunarion.db.local.Test.LunarServerHandlerTest;
import lunarion.node.EDF.NodeTaskCenter;
import lunarion.node.logger.Timer;
import lunarion.node.replicator.DBReplicator; 

/*
 * the server part is modified from the Netty user-guild:
 * http://netty.io/wiki/user-guide-for-5.x.html
 */
public class LunarServerStandAlone {
	
	private Logger logger = Logger.getLogger("LunarServerStandAlone"); 
	 
	private HashMap<String, LunarDB> db_map;
	private HashMap<String, DBReplicator> db_replicators;
	
	/*
	 * server processors
	 */ 
	private final int parallel = Runtime.getRuntime().availableProcessors() ; 
	int queue_upper_bound = 900;
	int queue_lower_bound = 100;
	protected  ExecutorService thread_executor = Executors.newFixedThreadPool(parallel); 
	EventLoopGroup bossGroup ;
    EventLoopGroup workerGroup ;
	
	private String server_root;
	private NodeTaskCenter node_tc;
	
	private static class LunarServerInstatance {
		private static final LunarServerStandAlone g_server_instance = new LunarServerStandAlone();
	}

	public static LunarServerStandAlone getInstance() {
		return LunarServerInstatance.g_server_instance;
	}
	
	public LunarServerStandAlone()
	{
		
	}
	
	private void initLogger(String name)
	{
		SimpleLayout layout = new SimpleLayout();
		 
    	FileAppender fa = null;
		try {
			fa = new  FileAppender(layout, name+".log.txt", true);
		} catch (IOException e) { 
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
        logger.addAppender(fa);
	}
	public void startServer(String __svr_root) throws IOException 
	{
		
		if(!__svr_root.endsWith("/"))
			server_root = __svr_root + "/";
		else
			server_root = __svr_root;
		
		String[] names = server_root.split("/");
		if(names.length>1)
			initLogger(server_root.split("/")[server_root.split("/").length-1]);
		else
			initLogger(names[0]);
		
		List<String> db_names = new ArrayList<String>();
		File dir = new File(server_root);
		if(!dir.isDirectory())
		{	
			logger.info(Timer.currentTime() + " [NODE ERROR]: unable to start server at: " + server_root);  
			logger.info(Timer.currentTime() + " [NODE ERROR]: the server root directory" + server_root + " is wrong, please start server with a correct directory");
			
			throw new IOException("[NODE ERROR]: the server root directory" + server_root + " is wrong, please start server with a correct directory");
		}
    	else
		{ 
    		File[] file_array=dir.listFiles( );
            if(file_array!=null)
            {
            	if(file_array.length == 0)
                {
                	 /*
                	  * do nothing, there is no db yet
                	  */
            		//System.out.println("[INFO]: there are no db on the server yet.");
            		logger.info(Timer.currentTime() + " [NODE ERROR]:there are no db on the server yet." );  
                }
            	else
            	{
            		for (int i = 0; i < file_array.length; i++) 
            		{ 
            			if(file_array[i].isDirectory())
            			{
            				 
                        	File db_i = new File(server_root+file_array[i].getName());
                        	String conf = server_root 
                        					+ file_array[i].getName()
                        					+ "/"
                        					+ DBFSProperties.runtime_conf;
                        	File conf_file = new File(conf);
                    		if(!conf_file.exists())
                    		{
                    			logger.info(Timer.currentTime() + " [NODE ERROR]: there is no db instance under this folder: " + server_root+file_array[i].getName() );  
                               
                    		}
                    		else
                    		{
                    			db_names.add(file_array[i].getName());
                    		}
            			} 
            		}
            	}
            }
		}
		 
		db_map = new HashMap<String, LunarDB>();
		db_replicators = new HashMap<String, DBReplicator>();
		
		for(int i=0;i<db_names.size();i++)
		{
			LunarDB i_db = new LunarDB();
			String db_root = server_root + db_names.get(i).trim(); 
			i_db.openDB(db_root);
			logger.info(Timer.currentTime() + " [NODE INFO]: database: " + i_db.dbName() + " is running now." );  
            
			DBReplicator replica = new DBReplicator(i_db);
			logger.info(Timer.currentTime() + " [NODE INFO]: database: " + i_db.dbName() + " has its replicator running now." );  
            
			db_map.put(db_names.get(i).trim(), i_db);
			db_replicators.put(db_names.get(i).trim(), replica);
			
		}
		
		
		node_tc = new NodeTaskCenter(this);
		bossGroup = new NioEventLoopGroup();
		workerGroup = new NioEventLoopGroup(); 
	} 

	public void closeServer()
	{
		bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        thread_executor.shutdown();
        
        Iterator<String> keys = db_map.keySet().iterator();
        while(keys.hasNext())
        {
        	boolean dbclosed = false;
        	String key = keys.next();
        	LunarDB db = db_map.get(key);
        	try {
				db.closeDB();
				dbclosed = true;
				db_replicators.get(key).close();
			} catch (IOException e) {
				logger.info(Timer.currentTime() + " [NODE ERROR]: fail to close database: " + key); 
				e.printStackTrace();
			} 
        	if(dbclosed)
        		 logger.info(Timer.currentTime() + " [NODE INFO]: database " + key + " has been shutdown successfully"); 
        }
        logger.info(Timer.currentTime() + " [NODE INFO]: server closed."); 
		
	}
	public void bind(int port) throws InterruptedException {
		
		 
		ServerBootstrap bootstrap = new ServerBootstrap();
		bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    //.childHandler(new ChildChannelHandler())
                    .childHandler(new LunarServerChannelInitializer(node_tc, logger))
                    .option(ChannelOption.SO_BACKLOG, 1024) 
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

		Channel ch = bootstrap.bind(port).sync().channel();
		System.err.println(Timer.currentTime() +  " DB node server channel binded at port: " + port + '.');
		logger.info(Timer.currentTime() + " [NODE INFO]:  DB node server channel binded at port: " + port + '.');     
		ch.closeFuture().sync();
            /*
             *  Bind and start to accept incoming connections.
             */
           // ChannelFuture future = bootstrap.bind(port).sync();
            /*
             *  Wait until the server socket is closed.
             *  In this example, this does not happen, but you can do that to gracefully 
             *  shut down your server.
             */
            
           // future.channel().closeFuture().sync();
        
    }
	
	
	public void submit(Runnable task ) {
       
        thread_executor.submit(task);
    } 
	
	public LunarDB getDBInstant(String db_name)
	{
		return this.db_map.get(db_name);
	}
	
	public static void main(String[] args) throws Exception {
	        int port = 9090;
	        if (args != null && args.length > 0) {
	            try {
	                port = Integer.valueOf(args[0]);
	            } catch (NumberFormatException e) {

	            }
	        }
	        
	        String svr_root = "/home/feiben/DBTest/LunarNode/";
			 
	        LunarServerStandAlone.getInstance().startServer(svr_root );
	        try {
	        	LunarServerStandAlone.getInstance().bind(port);
	        } finally { 
	        	LunarServerStandAlone.getInstance().closeServer(); 
	        }
	    }
}
