
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

import org.apache.log4j.Logger;

import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.utile.ControllerConstants;

public class FetchLog extends FetchRecords{
	
	public FetchLog( ) {
		super(false);
	}

	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD();
		 String[] params = (String[])msg.getParams();
		
		 if(params.length != 4)
		 {
			 MessageResponse response = null; 
			 System.err.println("[NODE ERROR]: wrong parameters for fetching log.");
			 response = ExecutorInterface.responseError(msg, MessageResponse.getNullStr(), MessageResponse.getNullStr(),CodeSucceed.wrong_parameter_count);
			 return response; 
		 }
		 params[1] = ControllerConstants.getLogTableName(params[1]); 
		
		 return fetchRecords(l_db_ssa, params, msg, logger);
	}

}
