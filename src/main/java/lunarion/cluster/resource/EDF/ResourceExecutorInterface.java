
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
package lunarion.cluster.resource.EDF;

import java.util.HashMap;

import org.apache.log4j.Logger;

import lunarion.cluster.resource.QueryEngine;
import lunarion.cluster.resource.Resource;
import lunarion.cluster.resource.ResourceDistributed;
import lunarion.cluster.resource.ResponseCollector;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;

public interface ResourceExecutorInterface { 

		public final int intermediate_result_uuid_index = 2;
		public final int table_name_index = 1;
		public final int db_name_index = 0; 
		 
		
		public ResponseCollector execute(QueryEngine db_res , String[] params , Logger logger) ;
		
		 
}
