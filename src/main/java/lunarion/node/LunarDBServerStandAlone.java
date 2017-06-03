
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
import java.util.concurrent.BlockingQueue;
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
import io.netty.channel.AdaptiveRecvByteBufAllocator;
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
import lunarion.node.logger.LoggerFactory;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.replicator.DBReplicator; 

/*
 * the server part is modified from the Netty user-guild:
 * http://netty.io/wiki/user-guide-for-5.x.html
 */
public class LunarDBServerStandAlone {
	
	private Logger logger = null; 
	
	/*
	 * <db_name, LunarDB instance>
	 */
	private HashMap<String, LunarDB> db_map;
	private HashMap<String, DBReplicator> db_replicators;
	/*
	 * <partition_name, queue of messages for this partition>
	 */
	private HashMap<String, BlockingQueue<String[]>> table_partition_notification_queue_map;
	
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
	
	/*
	private static class LunarServerInstatance {
		private static final LunarDBServerStandAlone g_server_instance = new LunarDBServerStandAlone();
	}

	public static LunarDBServerStandAlone getInstance() {
		return LunarServerInstatance.g_server_instance;
	}
	*/
	public LunarDBServerStandAlone()
	{
		
	}
	
	
	public void startServer(String __svr_root,Logger _logger) throws IOException 
	{
		
		if(!__svr_root.endsWith("/"))
			server_root = __svr_root + "/";
		else
			server_root = __svr_root; 
		
		logger = _logger;
		
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
		table_partition_notification_queue_map = new HashMap<String, BlockingQueue<String[]>>();
		
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
                    .childOption(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(64, 1024, 65536*512))
                    .option(ChannelOption.SO_BACKLOG, 1024) 
                    .option(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new LunarDBServerChannelInitializer(node_tc, logger));

		 
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
 
	public void registerRoutinTableWatcherQueue(String partition_name, BlockingQueue<String[]> table_partirion_update_notification_queue )
	{
		table_partition_notification_queue_map.put(partition_name, table_partirion_update_notification_queue);
	}
	
	public void notifyUpdate(String partition_name, String[] update_db_and_table)
	{
		BlockingQueue<String[]> bq = table_partition_notification_queue_map.get(partition_name);
		if(bq != null)
		{
			logger.info("[NODE INFO]: start notifying slave on partition " 
							+ partition_name 
							+ " to update data from "
							+ update_db_and_table[0]
							+ "."
							+ update_db_and_table[1]);
			
			bq.add(update_db_and_table);
			logger.info("[NODE INFO]: notified slave on partition " 
							+ partition_name 
							+ " to update data from "
							+ update_db_and_table[0]
							+ "."
							+ update_db_and_table[1]);
			
		}
		else
		{
			System.err.println("[NODE ERROR]: the queue for accepting update notification must not be null.");
			logger.info("[NODE ERROR]: the queue for accepting update notification must not be null.");
		}
		 
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
	        LunarDBServerStandAlone ldbssa = new LunarDBServerStandAlone();
	        ldbssa.startServer(svr_root, LoggerFactory.getLogger("LunarNode"));
	        try {
	        	ldbssa.bind(port);
	        } finally { 
	        	ldbssa.closeServer(); 
	        }
	        /*
	        LunarDBServerStandAlone.getInstance().startServer(svr_root, LoggerFactory.getLogger("LunarNode"));
	        try {
	        	LunarDBServerStandAlone.getInstance().bind(port);
	        } finally { 
	        	LunarDBServerStandAlone.getInstance().closeServer(); 
	        }*/
	    }
}
