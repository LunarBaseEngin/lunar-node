
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
import lunarion.node.page.DataPage;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.MessageResponseForQuery;

public class FTQuery implements ExecutorInterface{

	private ConcurrentHashMap<String, FTQueryResult> result_map;
	
	public FTQuery(  ConcurrentHashMap<String, FTQueryResult> _result_map)
	{
		result_map = _result_map;
	}
	
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD();
		 
		
		 return ftQuery(l_db_ssa, msg, logger);
	}
	
	private MessageResponse ftQuery(LunarDBServerStandAlone l_db_ssa , MessageRequest request, Logger logger)
    {
		String[] params = (String[])request.getParams();
   	 
    	MessageResponse response = null;
    	if(params.length < 3)
		{
			System.err.println("[NODE ERROR]: wrong parameters for a query request.");
			response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(),CodeSucceed.wrong_parameter_count);
			return response;
		}
		String db = params[0];
        String table = params[table_name_index];
        String statement = params[2];
        //int from = Integer.parseInt(params[3]);
        //int count = Integer.parseInt(params[4]);
         
		//return node_tc.dispatch(new VNodeIncomingRecords(db,table,recs));
        LunarDB l_DB = l_db_ssa.getDBInstant(db);
        if(l_DB == null)
        {   
        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.db_does_not_exist);
        	return response;
        }
        
        
        LunarTable t_table = l_DB.getTable(table);
        if(t_table == null)
        {
        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.table_does_not_exist);
        	return response;  
        }
        
        int latest_count = 0; //get all records of the query.
        FTQueryResult result = null;
        ArrayList<Record32KBytes> recs = null;
         
        result = l_DB.queryFullText(table, statement, latest_count);
		 
        if(result != null && result.resultCount() >0 )
        {  
        	 
        	String[] result_uuid = ExecutorInterface.constructQueryResultHandler(l_DB, db,  table, result); 
        	
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
		  	
        return response ;
		   
    }
    
}
