
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
package lunarion.cluster.resource;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.spi.SelectorProvider;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;
import org.omg.CORBA.portable.InputStream;

import LCG.FSystem.Manifold.LFSDirStore;
import LCG.FSystem.Manifold.NameFilter;
import LCG.StorageEngin.IO.L1.IOStream;
import LCG.StorageEngin.IO.L1.IOStreamNative;
import io.netty.channel.ChannelFuture;
import lunarion.cluster.coordinator.adaptor.LunarDBSchema;
import lunarion.cluster.resource.EDF.ResourceExecutorCenter;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command;
import lunarion.node.LunarNode;
import lunarion.node.logger.LoggerFactory;
import lunarion.node.logger.Timer;
import lunarion.node.page.DataPage;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;
import lunarion.node.requester.MessageClientWatcher;
import lunarion.node.utile.ControllerConstants;
import lunarion.node.utile.Screen;

/*
 * for one resource(one db in other words in our case), if we have 3 nodes, 
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
public class ResourceDistributed extends Resource{
	 
	
	
	
	public ResourceDistributed(HelixAdmin _admin, String _cluster_name, String _res_name, 
					int _num_partitions, int _num_replicas, 
					int _max_rec_per_partition,
					String _meta_files_path,
					String _model_file) throws IOException
	{
		super( _admin, _cluster_name, _res_name, _num_partitions, _num_replicas, 
					_max_rec_per_partition,
					_meta_files_path,
					_model_file); 
		
		
	} 
	
	public ResponseCollector queryRemoteWithFilter(String table, String logic_statement)
	{ 
		String[] params = new String[3];
		params[0] = this.resource_name;
		params[1] = table;
		params[2] = logic_statement;
		return sendRequest(CMDEnumeration.command.filterForWhereClause, params );
	}
	
	public ResponseCollector sendRequest(CMDEnumeration.command cmd, String[] params )
	{
		return re_center.dispatch(cmd, params); 
	}
	 
}
