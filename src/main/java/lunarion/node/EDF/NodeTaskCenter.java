
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

import java.io.IOException;

import LCG.DB.API.LunarDB;
import LCG.DB.EDF.Events.Delete; 
import LCG.DB.EDF.Events.OrderedSets;
import LCG.DB.EDF.Events.QueryAnd;
import LCG.DB.EDF.Events.QueryLatestN;
import LCG.DB.EDF.Events.QueryRange;
import LCG.DB.EDF.Events.QueryRecs;
import LCG.DB.EDF.Events.QueryResult;
import LCG.DB.EDF.Events.QuerySimple;
import LCG.DB.EDF.Events.QuerySimpleIDs;
import LCG.DB.EDF.Events.Update; 
import LCG.EnginEvent.EventDispatcher;
import lunarion.node.LunarServerStandAlone;
import lunarion.node.EDF.events.VNodeIncomingRecords;

public class NodeTaskCenter extends EventDispatcher{
	  
	final LunarServerStandAlone l_server;
	 
	public NodeTaskCenter(LunarServerStandAlone _server ) {
		 
		l_server = _server;
		 
		registerHandler(VNodeIncomingRecords.class, new VNodeTaskInsert(this));
		
		/*
		registerHandler(QueryResult.class, new TaskPrint());
			registerHandler(OrderedSets.class, new TaskIntersect(this, set_operator));
			
			registerHandler(QuerySimple.class, new TaskQuery(this));
			registerHandler(QuerySimpleIDs.class, new TaskQueryIDs(this));
			registerHandler(QueryAnd.class, new TaskQueryAnd(this));
			registerHandler(QueryLatestN.class, new TaskLatestN(this));
			registerHandler(QueryRange.class, new TaskQueryRange(this));
			
			registerHandler(QueryRecs.class, new TaskFetchRecs(this));
			
			registerHandler(Delete.class, new TaskDelete(this));
			registerHandler(Update.class, new TaskUpdate(this));
			
		 */
	} 
	
	public LunarServerStandAlone getActiveServer()
	{
		return this.l_server;
	}
	 
}
