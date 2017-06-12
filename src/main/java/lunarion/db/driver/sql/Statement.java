
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
package lunarion.db.driver.sql;

import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.requester.LunarDBClient;

public class Statement {
	
	 
	private String db;
	 
	private  LunarDBClient client ;
	
	public Statement(LunarDBClient _client, String _db)
	{
		this.client = _client;
		this.db = _db;
	}
	
	public LResultSet createTable(String table_name) throws InterruptedException
	{
		CMDEnumeration.command cmd = CMDEnumeration.command.createTable;
    	String[] params = new String[2];
    	params[0] = db; 
    	params[1] = table_name.trim(); 
		RemoteResult rr = client.sendRequest(cmd, params, 50*1000);
		
		return new LResultSet(client, rr);
	}
	
	/*
	 * @params type:
	 * text: will be treated as a fulltext column that we can query by keywords. 
	 * 		e.g. select * from table_1 where content like 'ok + today, book'
	 * 		Lunarbase uses its own search engine to enhance the query capability with keywords in sql.
	 * 
	 * string: same as text.
	 * 
	 * varchar: typically used as columns like name, favorite, title, which are searchable by their values.
	 * 		e.g. select * from table_1 where name = 'michael'
	 * 
	 * long: supports point query and range query.
	 * 		e.g. select * from table_1 where age < 90 and salary > 10000
	 * 
	 * int(to do) 
	 * time(to do): will be transformed to a long for range query.
	 */
	// replaced by indexColumn to clarify its functionality to programmers.
	 
	public LResultSet indexColumn(String table_name, String column, String type) throws InterruptedException
	{
		CMDEnumeration.command cmd = CMDEnumeration.command.addAnalyticColumn;
    	String[] params = new String[4];
    	params[0] = db; /* db */
    	params[1] = table_name; /* table */
    	params[2] = column ; /* column needs to be analytical, i.e can do range query and other arithmetic operation */
    	params[3] = type	; /* column type in long */
    	
    	RemoteResult rr = client.sendRequest(cmd, params, 15*1000);
		
		return new LResultSet(client, rr);
	}
	
	
	public LResultSet addFulltextColumn(String table_name, String column ) throws InterruptedException
	{  
		CMDEnumeration.command cmd = CMDEnumeration.command.addFulltextColumn;
		String[] params = new String[3];
		params[0] = db; /* db */
		params[1] = table_name; /* table */
		params[2] = column ; /* column needs to be fulltext searched */
	 
    	RemoteResult rr = client.sendRequest(cmd, params, 15*1000);
		
		return new LResultSet(client, rr);
	} 
	
	public LResultSet addStorableColumn(String table_name, String column ) throws InterruptedException
	{  
		CMDEnumeration.command cmd = CMDEnumeration.command.addStorableColumn;
		String[] params = new String[3];
		params[0] = db; /* db */
		params[1] = table_name; /* table */
		params[2] = column ; /* column needs to be stored */
		 
    	RemoteResult rr = client.sendRequest(cmd, params, 15*1000);
		
		return new LResultSet(client, rr);
	} 
	
	public LResultSet insert(String table_name, String[] recs) throws InterruptedException
	{
		CMDEnumeration.command cmd = CMDEnumeration.command.insert;
    	String[] params = new String[recs.length+2];
    	params[0] = db; 
    	params[1] = table_name.trim(); 
    	for(int i = 0;i<recs.length;i++)
    		params[i+2] = recs[i];
    	
		RemoteResult rr = client.sendRequest(cmd, params, 15*1000);
		
		return new LResultSet(client, rr);
	}
	
	
	public LResultSet executeQuery(String statement) throws InterruptedException
	{
		CMDEnumeration.command cmd = CMDEnumeration.command.sqlSelect;
    	String[] params = new String[2];
    	params[0] = db; 
    	params[1] = statement;
		RemoteResult rr = client.sendRequest(cmd, params, 500*1000);
		
		return new LResultSet(client, rr);
	}

}
