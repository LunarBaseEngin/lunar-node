
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

import java.util.concurrent.ExecutionException;

import LCG.DB.API.LunarDB;
import LCG.DB.EDF.Events.IncommingRecords;
import LCG.EnginEvent.Event;
import LCG.EnginEvent.Interfaces.LHandler;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import lunarion.node.EDF.events.VNodeIncomingRecords;

public class VNodeTaskInsert implements LHandler<Event, Record32KBytes[]> { 
	
	 
	private final NodeTaskCenter _node_task_center;
	  
	  public VNodeTaskInsert(NodeTaskCenter _node_tc) {
		  _node_task_center = _node_tc; 
	  } 
	  
	  public Record32KBytes[] execute(Event evt) {
	    if (evt.getClass() == VNodeIncomingRecords.class) {
	    	VNodeIncomingRecords recs = (VNodeIncomingRecords) evt;
	    	return internalExecute(recs._db, recs._table,  recs._records);
	    }
	    return null;
	  } 
	 
	  private Record32KBytes[] internalExecute(String db, String table, String[] __recs) 
	  {
		  LunarDB l_DB = _node_task_center.getActiveServer().getDBInstant(db);
		  Record32KBytes[] respond = l_DB.insertRecord(table, __recs);
		  
		  return respond;
		 
	  } 
}
