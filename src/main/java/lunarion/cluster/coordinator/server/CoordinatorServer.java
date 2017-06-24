
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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import LCG.DB.API.LunarDB;
import LCG.FSystem.Def.DBFSProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lunarion.cluster.coordinator.Coordinator;
import lunarion.cluster.resource.Resource;
import lunarion.node.LunarDBServerChannelInitializer; 
import lunarion.node.logger.LoggerFactory;
import lunarion.node.logger.Timer;
import lunarion.node.replicator.DBReplicator;
import lunarion.node.utile.ControllerConstants;

public class CoordinatorServer {

	private Logger logger = null;  
	 
	AtomicBoolean started = new AtomicBoolean(false);
	/*
	 * server processors
	 */ 
	private final int parallel = Runtime.getRuntime().availableProcessors() ; 
	int queue_upper_bound = 900;
	int queue_lower_bound = 100;
	protected  ExecutorService thread_executor = Executors.newFixedThreadPool(parallel); 
	EventLoopGroup bossGroup ;
    EventLoopGroup workerGroup ;
	
	 
	private Coordinator co;
	ControllerConstants cc = new ControllerConstants(); 
	
	String zkAddr = null;
	String cluster_name = null;
	//String resource_name = null; /* used as db name */
	//int num_partition = 6;
	//int num_replicas = 2;
	
	 
	private static class CoordinatorServerInstatance {
		private static final CoordinatorServer g_server = new CoordinatorServer();
	}

	public static CoordinatorServer getInstance() {
		return CoordinatorServerInstatance.g_server ;
	}
	 
	public CoordinatorServer()
	{
		
	}
	
	public boolean started()
	{
		return this.started.get();
	}
	
	public Resource getResource(String resource_name)
	{
		return co.getResource(resource_name);
	}
	
	public void printState(String prompt, String resource_name)
	{
		co.printState(prompt, resource_name);
	}
	public void addNode(String resource_name, String node_ip, int port) throws Exception
	{
		co.addNodeToResource(resource_name, node_ip, port);
	}
	
	public void startServer(String _zk_addr, 
							String _cluster_name, 
							String _res_name, 
							int _partitions, 
							int _replicas,
							int _max_recs_per_partition,
							String _meta_file,
							String _model_file) throws IOException 
	{
		zkAddr = _zk_addr;

		cluster_name = _cluster_name;
		String resource_name = _res_name;
		int num_partition = _partitions ;
		int num_replicas = _replicas ;
		int max_recs_per_partition = _max_recs_per_partition;
		String meta_file = _meta_file;
		String mode_file = _model_file;
		
		logger = LoggerFactory.getLogger("coordinator"); 
	    
	    logger.info(Timer.currentTime() + " [INFO]: coordinator is starting for cluster " + _cluster_name);
	 	 
	    co = Coordinator.getInstance();
	    co.init(zkAddr,cluster_name, cc);
	    co.startZookeeper();
	    co.setup();
	    co.addResource(resource_name, num_partition,num_replicas, max_recs_per_partition, meta_file,mode_file);
	    co.startController();
		// co.printState("State after starting the coordinator: ", resource_name);
		
	    try {
			Class.forName("org.apache.calcite.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}
		 
		 
		bossGroup = new NioEventLoopGroup();
		workerGroup = new NioEventLoopGroup(); 
		
		logger.info(Timer.currentTime() + " [INFO]: coordinator started. ");
		started.set(true);
	}
	
	public boolean addNodeToResource(String resource_name, String node_ip, int node_port)
	{
		 try {
			return co.addNodeToResource(resource_name, node_ip, node_port);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			return false;
		}
	}
	
	public void updateMasters(String resource)
	{
		 Resource res = co.getResource(resource);
		 res.updateMasters( ) ;
	}

	public void closeServer()
	{
		bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        thread_executor.shutdown();
       
        logger.info(Timer.currentTime() + " [COORDINATOR INFO]: coordinator server closed."); 
		
	}
	public void bind(int port) throws InterruptedException {
		
		 
		ServerBootstrap bootstrap = new ServerBootstrap();
		bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(64, 1024, 65536*512)) 
                    //.childHandler(new ChildChannelHandler())
                    .childHandler(new CoordinatorServerChannelInitializer(co, logger))
                    .option(ChannelOption.SO_BACKLOG, 1024) 
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

		Channel ch = bootstrap.bind(port).sync().channel();
		System.err.println(Timer.currentTime() +  "[INFO]: coordinator server channel binded at port: " + port + '.');
		logger.info(Timer.currentTime() + " [COORDINATOR INFO]:  coordinator server channel binded at port: " + port + '.');     
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
		 
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
