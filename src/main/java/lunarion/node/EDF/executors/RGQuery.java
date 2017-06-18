
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

import java.util.ArrayList;
import java.util.UUID;
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

public class RGQuery implements ExecutorInterface{

	private ConcurrentHashMap<String, FTQueryResult> result_map;
	
	public RGQuery(  ConcurrentHashMap<String, FTQueryResult> _result_map)
	{
		result_map = _result_map;
	}
	
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD(); 
		
		 return rgQuery(l_db_ssa, msg, logger);
	}
	
	private MessageResponse rgQuery(LunarDBServerStandAlone l_db_ssa , MessageRequest request, Logger logger)
	{
		String[] params = (String[])request.getParams();
	   	 
    	MessageResponse response = null;
    	
	    	if(params.length < 7)
			{
				System.err.println("[NODE ERROR]: wrong parameters for a query request.");
				response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(), CodeSucceed.wrong_parameter_count);
				return response;
			}
			String db = params[0];
	        String table = params[table_name_index];
	        String column = params[2];
	        long lower = Long.parseLong( params[3]);
	        long upper = Long.parseLong( params[4]);
	        int lower_inclusive = Integer.parseInt(params[5]);
	        int upper_inclusive = Integer.parseInt(params[6]);
	         
	        boolean l_i = lower_inclusive == 1?true:false;
	        boolean u_i = upper_inclusive == 1?true:false;
	        
			 
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
	        
	        FTQueryResult result = null;
	        ArrayList<Record32KBytes> recs = null;
	         
	        result = l_DB.queryRange(table, column, lower, upper, l_i, u_i); 
			 
	        if(result != null && result.resultCount()>0)
	        {
	        	
	        	String[] result_uuid = new String[5];
	        	result_uuid[0] = db;
	        	result_uuid[table_name_index] = table;
	        	result_uuid[intermediate_result_uuid_index] = UUID.randomUUID().toString();
	        	result_uuid[3] = ""+result.resultCount();
	        	result_uuid[4] = "0";
	        	
	        	//recs = result.fetchRecords(from, count);
				response = new MessageResponseForQuery();
				response.setUUID(request.getUUID());
			    response.setCMD(request.getCMD());
			    response.setSucceed(true);
				//response.setParams(recs);
			    response.setParams(result_uuid);	
				result_map.put(result_uuid[intermediate_result_uuid_index], result);
	        }
	        else
	        {
	        	response = new MessageResponseForQuery();
	        	response.setUUID(request.getUUID());
	        	response.setCMD(request.getCMD());
	        	response.setSucceed(false);
	        	response.setParamsFromNode(db, table, recs);
	        } 
			  	
	        return response;
			   
	    }
}
