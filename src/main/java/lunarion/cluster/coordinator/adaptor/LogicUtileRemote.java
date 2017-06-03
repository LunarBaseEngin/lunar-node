
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
package lunarion.cluster.coordinator.adaptor;

 
import lunarion.cluster.coordinator.Resource;
import lunarion.cluster.coordinator.ResponseCollector;
import lunarion.cluster.coordinator.adaptor.converter.TripleOperator; 

public class LogicUtileRemote extends LogicUtile{
	
	final Resource db_resource;
	
	public LogicUtileRemote(String[] _cols, Resource _db_resource, String _table)
	{
		super(null, _table, _cols );
		 
		db_resource = _db_resource; 
	}
	
	public ResponseCollector queryRemote(TripleOperator t_o)
	{
		String statement = makeQuery(t_o);
		
		return queryRemote(statement);
	}
	
	public ResponseCollector queryRemote(String filter_statement)
	{  
		
		return db_resource.queryRemoteWithFilter(table_name, filter_statement) ;
		
	}
	

}
