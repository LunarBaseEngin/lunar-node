
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
package lunarion.node.EDF.executors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.API.Result.FTQueryResult;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForQuery;

public class FetchQueryResultRecs implements ExecutorInterface{

	private ConcurrentHashMap<String, FTQueryResult> result_map;
	
	public FetchQueryResultRecs(  ConcurrentHashMap<String, FTQueryResult> _result_map)
	{
		result_map = _result_map;
	}
	
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD(); 
		
		 return fetchQueryResultRecs(l_db_ssa, msg, logger);
	}
	
	private MessageResponse fetchQueryResultRecs(LunarDBServerStandAlone l_db_ssa , MessageRequest request, Logger logger)
	{
		String[] params = (String[])request.getParams();
	   	 
    	MessageResponse response = null;
    	
		 
	    	if(params.length != 5)
			{
				System.err.println("[NODE ERROR]: wrong parameters for fetching query result.");
				response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(), CodeSucceed.wrong_parameter_count);
				return response;
			}
			String db = params[0];
	        String table = params[table_name_index];
	        String query_result_uuid = params[2];
	         
	        long l_from = Long.parseLong(params[3]);
	        if(l_from > Integer.MAX_VALUE)
	        {	
	        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.rec_id_out_of_boundary);
	        	return response;
	        }
	    	
	        int from = (int) l_from;
	        
	        int count = Integer.parseInt(params[4]); 
			 
	        LunarDB l_DB = l_db_ssa.getDBInstant(db);
	        if(l_DB == null)
	        {   
	        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.db_does_not_exist);
	        	return response;
	        }
	        
	        
	        LunarTable t_table = l_DB.getTable(table);
	        if(t_table == null)
	        {
	        	response = ExecutorInterface.responseError(request, db,table, CodeSucceed.table_does_not_exist); 
	        	return response;
	        } 
	        
	        FTQueryResult result = this.result_map.get(query_result_uuid);
	        
	        ArrayList<Record32KBytes> recs = null; 
			 
	        if(result != null )
	        { 
	        	try {
					recs = result.fetchRecords(from, count);
					response = new MessageResponseForQuery();
					response.setUUID(request.getUUID());
				    response.setCMD(request.getCMD());
				    response.setSucceed(true);
					response.setParamsFromNode(db, table, recs); 
				} catch (IOException e) { 
					response = ExecutorInterface.responseError(request, db,table, CodeSucceed.exception_when_fetching_query_result_records);  
				   
				  	
					e.printStackTrace();
				}
				
	        }
	        else
	        {
	        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.has_null_result_for_the_given_queryresultuuid);  
	        	return response;
	        } 
			  	
	        return response;
			   
	    }
	    
	    
}
