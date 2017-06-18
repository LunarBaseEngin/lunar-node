
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

import org.apache.log4j.Logger;

import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.remote.protocol.Message;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public interface ExecutorInterface {

	public final int intermediate_result_uuid_index = 2;
	public final int table_name_index = 1;
	public final int db_name_index = 0;
	    
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger);
	
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
	    
}
