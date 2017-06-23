
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List; 
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger; 
import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.API.DBStatus.DBRuntimeStatus;
import LCG.DB.API.Result.FTQueryResult;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine; 
import LCG.RecordTable.StoreUtile.Record32KBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import lunarion.cluster.coordinator.Resource;
import lunarion.cluster.coordinator.ResponseCollector;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command; 
import lunarion.node.logger.LogCMDConstructor;
import lunarion.node.logger.TableOperationLogger;
import lunarion.node.logger.Timer; 
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForDriver;
import lunarion.node.remote.protocol.MessageResponseForQuery;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.MessageClientWatcher;
import lunarion.node.utile.ControllerConstants;

public class TaskRedirectMessage implements Runnable {
 	
    private MessageRequest request = null;
    private MessageResponse response = null;
     
	private ChannelHandlerContext ctx = null;
	private Resource db_resource;
    private Logger logger= null;
    
    final int intermediate_result_uuid_index = 2;
    final int table_name_index = 1;
    final int db_name_index = 0;
    
    private ConcurrentHashMap<String, ResponseCollector> response_map =  null;

    
    public MessageResponse getResponse() {
        return response;
    }

    public MessageRequest getRequest() {
        return request;
    }

    public void setRequest(MessageRequest _request) {
        this.request = _request;
    }

    TaskRedirectMessage(MessageRequest _request , 
    						Resource _db_resource, 
    						ChannelHandlerContext ctx, 
    						Logger _logger,
    						ConcurrentHashMap<String, ResponseCollector> _response_map) {
       
    	this.request = _request; 
      
        this.db_resource = _db_resource;
        this.ctx = ctx;
        this.logger = _logger; 
        this.response_map = _response_map;
    	
    }
    
    TaskRedirectMessage(MessageRequest request , 
    					Resource _db_resource, 
						Logger _logger) {
    		this.request = request; 
    	 
    		this.db_resource = _db_resource;
    		this.ctx = null;
    		this.logger = _logger; 
    }

    public void run() { 
       
        execute(request); 
        
        if(ctx!=null)
        {
        	 int len = response.size();
             ByteBuf response_buff = Unpooled.buffer(4+len);
             response_buff.writeInt(len);
             response.write(response_buff);
           
             ctx.writeAndFlush(response_buff).addListener(new ChannelFutureListener() {
                 public void operationComplete(ChannelFuture channelFuture) throws Exception {
                     //System.out.println("[NODE INFO]: LunarNode responds the request with message id:" + request.getUUID());
                 }
             });
        }
       
    }

    private void execute(MessageRequest request) {
        
        CMDEnumeration.command cmd = request.getCMD();
        String[] params = (String[])request.getParams();
        
        ResponseCollector rc = null;
    	
        switch(request.getCMD())
		{
		case createTable:
		case addFulltextColumn:
		case addAnalyticColumn:
		case addStorableColumn:
		case insert:
		{
			if(db_resource == null)
			{
				responseError(params[0], params[1], CodeSucceed.db_does_not_exist);
				return;
			}
			if(request.getCMD() == CMDEnumeration.command.addAnalyticColumn)
			{
				/*
				 * @TaskHandlingMessage.addFunctionalColumn()
				 */
				if(params.length < 4)
				{	
					System.err.println("[COORDINATOR ERROR]: addAnalyticColumn needs at least 4 parameters: db name, table name, column name and column type");
					responseError(params[0],params[1], CodeSucceed.wrong_parameter_count);
					return;
				}
				String column_type =  params[3];
				if(!DataTypeSupported.supported(column_type))
				{
					System.err.println("[COORDINATOR ERROR]: addAnalyticColumn fail to add analytic column with unsupported data type");
					responseError(params[0],params[1], CodeSucceed.add_analytic_column_with_unsupported_datatype_failed);
					return;
				}
					
			}
			rc = db_resource.sendRequest(request.getCMD(), request.getParams());
			if(rc == null)
			{
				responseError(params[0], params[1], CodeSucceed.wrong_parameter_count);
				return;
				
			}
			response = new MessageResponseForDriver();
			response.setCMD(request.getCMD());
			response.setUUID(request.getUUID());
			response.setSucceed(rc.isSucceed()); 
			ArrayList<String> resp_for_driver = new ArrayList<String>();
			
			//if(request.getCMD() == CMDEnumeration.command.createTable)
			//{	
			 
				//resp_for_driver.add(params[0]);
				//resp_for_driver.add(params[1]); 
				 
				Iterator<Integer> all_table_partitions = rc.getAllDataPieces();
				while(all_table_partitions.hasNext())
				{
					Integer table_partition_i = all_table_partitions.next();
					RemoteResult rr = rc.getRemoteResult(table_partition_i);
					/*
					 * see what returns from 
					 * {@link TaskHandlingMessage#createTable(String[] params) createTable}. 
					 */
					//if(rr.isSucceed())
						resp_for_driver.add(rr.getParams()[2]);
					//else
					//	resp_for_driver.add(rr.getParams()[0]);
					 
				}
			//} 
			response.setParamsFromCoordinator(db_resource.getDBName(), MessageResponseForQuery.getNullStr(), resp_for_driver);  
		}
		break;
		case sqlSelect: 
		{
			rc = db_resource.sendRequest(cmd, params); 
			String[] result_with_intermediate_uuid = new String[5];
			result_with_intermediate_uuid[0] = db_resource.getDBName();
			result_with_intermediate_uuid[table_name_index] = MessageResponse.getNullStr();
			result_with_intermediate_uuid[intermediate_result_uuid_index] = UUID.randomUUID().toString();
			result_with_intermediate_uuid[3] = ""+rc.resultCount();
			result_with_intermediate_uuid[4] = "0";
        	
        	 
			response = new MessageResponseForDriver();
			response.setUUID(request.getUUID());
		    response.setCMD(request.getCMD());
		    response.setSucceed(true); 
		    response.setParams(result_with_intermediate_uuid);	
		    response_map.put(result_with_intermediate_uuid[intermediate_result_uuid_index], rc);
				
		}
		break;
		case fetchQueryResultRecs:
		{
			/*
			 * @RemoteResult.fetchQueryResult( long from, int count) 
			 */
			String db  = params[0];
			String table = params[1];
			String intermediate_uuid = params[2];
			long from =  Long.parseLong(params[3]);
			int count = Integer.parseInt(params[4]); 
			rc = response_map.get(intermediate_uuid);
			if(rc != null)
			{
				ArrayList<String> recs = rc.getRecordsForCMDQuery(null, from, count);
				if(!recs.isEmpty())
				{
					response = new MessageResponseForDriver();
					response.setUUID(request.getUUID());
					response.setCMD(request.getCMD());
					response.setSucceed(rc.isSucceed()); 
					response.setParamsFromCoordinator(db_resource.getDBName(), MessageResponseForQuery.getNullStr(), recs);
					return;
				}
				else
				{
					/*
					 * response error as @TaskHandlingMessage.fetchQueryResultRecs(String[] params)
					 */ 
				  	responseError(db, table, CodeSucceed.nomore_records_in_resultset);
					return;
				}
			}
			/*
			 * response error as @TaskHandlingMessage.fetchQueryResultRecs(String[] params)
			 */ 
			responseError(db, table, CodeSucceed.has_null_result_for_the_given_queryresultuuid); 
			 
		}
		break;
		case closeQueryResult:
		{
			/*
			 * @RemoteResult.closeQuery();
			 */
			if(params.length <3 )
			{
				System.err.println("[NODE ERROR]: wrong parameters for closing a query result.");
				if(db_resource == null)
					responseError(MessageResponse.getNullStr(), MessageResponse.getNullStr(), CodeSucceed.wrong_parameter_count);
				else
					responseError(db_resource.getDBName(), MessageResponse.getNullStr(), CodeSucceed.wrong_parameter_count);
				
				return ;
			}
			String db = params[0];
	        String table = params[table_name_index];
	        String query_result_uuid = params[2];
	        
	        ResponseCollector rc_removed = this.response_map.remove(query_result_uuid);
	        if(rc_removed!= null)
	        {  
	        	response = new MessageResponse();
			  	response.setUUID(request.getUUID());
			  	response.setCMD(request.getCMD());
			  	response.setSucceed(true); 
			  	String[] resp = new String[3];
			  	resp[0] = db; 
			  	resp[1] = table;  
			  	resp[2] = CodeSucceed.result_removed_succeed; 
			  	response.setParams(resp); 
	        } 
	        else
	        {
	        	responseError(db, table, CodeSucceed.result_removed_failed);
			 
	        	return ;
	        } 
	        	
		}
		break;
		default:
		{
			response = new MessageResponse();
			response.setCMD(request.getCMD());
			response.setUUID(request.getUUID());
			response.setSucceed(rc.isSucceed()); 
			
			String[] resp_message = new String[2];
			resp_message[0] = UUID.randomUUID().toString();
			resp_message[1] = CodeSucceed.unknown_cmd;
    			
			response.setParams(resp_message);	
		}
		break;
		}
			
		
		if(CMDEnumeration.needNotify(request.getCMD()))
			db_resource.notifySlavesUpdate(rc); 
			
     
    } 
      
	private void responseError(String db, String table, String error_code)
    {
    	response = new MessageResponse();
	  	response.setUUID(request.getUUID());
	  	response.setCMD(request.getCMD());
	  	response.setSucceed(false); 
	  	String[] resp = new String[3];
	  	resp[0] = db;
	  	resp[1] = table; 
	  	resp[2] = error_code;
	  	response.setParams(resp); 
    } 
    
    
}
