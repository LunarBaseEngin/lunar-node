
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
package lunarion.node.EDF;

import java.util.UUID;

import org.apache.log4j.Logger;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.API.Result.FTQueryResult;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import lunarion.cluster.resource.ResourceDistributed;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.page.DataPage;
import lunarion.node.remote.protocol.MessageToWrite;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public interface ExecutorInterface {

	public final int intermediate_result_uuid_index = 2;
	public final int table_name_index = 1;
	public final int db_name_index = 0;
	    
	 
	
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger) ;
	
	public static MessageResponse responseError(MessageRequest request, String db, String table, String error_code)
	{
		MessageResponse response = new MessageResponse();
		response.setUUID(request.getUUID());
		response.setCMD(request.getCMD());
		response.setSucceed(false); 
		String[] resp = new String[3];
		resp[0] = db;
		resp[1] = table; 
		resp[2] = error_code;
		response.setParams(resp); 
		
		return response;
	}
	
	public static String[] constructQueryResultHandler(LunarDB db, String db_name, String table, FTQueryResult query_result)
	{
		int max_level = 0;
		if(db.getTable(table).recordsCount() > 0) 
			max_level =  (db.getTable(table).recordsCount()-1 & DataPage.data_page_mask) >> DataPage.data_page_bit_len ;
    	
		int[] pages_with_rec_count = DataPage.calcRecsInPages2(query_result.getRecIDs(), max_level);
    	/*
    	 * result_uuid[0] = db.dbName();
    	 * result_uuid[table_name_index] = table;
    	 * result_uuid[intermediate_result_uuid_index] = UUID.randomUUID().toString();
    	 * result_uuid[3] = ""+query_result.resultCount();
    	 * 
    	 * result_uuid[4] = records in the page on level 0;
    	 * result_uuid[5] = records in the page on level 1;
    	 * ...
    	 * result_uuid[max_level] = records in the page on level 1;
    	 */
    	String[] result_uuid = new String[4 + pages_with_rec_count.length];
    	result_uuid[0] = db_name;
    	result_uuid[table_name_index] = table;
    	result_uuid[intermediate_result_uuid_index] = UUID.randomUUID().toString();
    	result_uuid[3] = ""+query_result.resultCount();
    	//TODO do not transfer integer to string, instead using binary protocol to serialize the object. 
    	for(int j = 0; j < pages_with_rec_count.length; j++ )
    	{
    		result_uuid[4+j] = pages_with_rec_count[j]+""; 
    	}
    	
    	return result_uuid;
	}
	    
}
