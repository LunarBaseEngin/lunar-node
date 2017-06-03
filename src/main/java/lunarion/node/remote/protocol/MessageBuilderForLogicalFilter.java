
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
package lunarion.node.remote.protocol;

import org.apache.calcite.rex.RexNode;

import lunarion.cluster.coordinator.adaptor.LogicUtileRemote;

 
public class MessageBuilderForLogicalFilter extends LogicUtileRemote{
	
	String[] columns; 
	String db_name;
	String table_name;
	 
	/*
	 * RexNode comes from calcite, representing algebraic logic: AND, OR 
	 * or simple binary logic: col <100, and their combination
	 */
	public MessageBuilderForLogicalFilter(String[] _columns, String _db_name, String _table)
	{
		 super(null, null, null);
		 columns = _columns;
		 db_name =_db_name;
		 table_name = _table;
	}
	
	
	
	

}
