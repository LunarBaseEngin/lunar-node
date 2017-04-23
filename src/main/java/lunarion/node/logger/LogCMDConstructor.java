
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
package lunarion.node.logger;

import lunarion.db.local.shell.CMDEnumeration.command;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.ReservedSymbols;

public class LogCMDConstructor {

	public static final String col_db_name = "db_name";
	public static final String col_table_name = "table_name";
	public static final String col_cmd = "cmd";
	public static final String col_param = "params";
	
	public static final String param_delim = ReservedSymbols.value_for_rec_column_delim;
	
	public static String getLogTableName(String table_name)
	{
		return table_name + "_log";
	}
	
	static public String patchParams(String[] params )
	{
		if(params.length==0)
			return "";
		
		String str  = params[0];
		for(int i=1;i<params.length;i++)
		{
			str += param_delim + params[i];
		}
		
		return str;
	}
	public static String contructLogCreate(String db, String table)
	{  
		return "{" + col_cmd + "=" + command.createTable.getByte() + ","
					+ col_param + "=" + db + param_delim + table
				+ "}";
	}
	
	public static String[] contructLogInsert(String db, String table, String[] records)
	{  
		String[] cmd_set = new String[2*records.length];
		for(int i=0;i < records.length;i++)
		{
			cmd_set[i*2] = "{" + col_cmd + "=" + command.insert.getByte() + ","
					+ col_param + "=" + db + param_delim + table
					+ "}";
			cmd_set[i*2+1] = records[i];
		}
		
		
		return cmd_set;
	}
	
	public static String contructLogAddingFulltextColumn(String db, String table, String col )
	{
		String rec =  "{" + col_cmd + "=" + command.addFulltextColumn.getByte() + ","
							+ col_param + "=" + db + param_delim + table + param_delim
							+  col 
							+ "}";
					
		return rec;
		
	}
	
	public static String contructLogAddingFulltextColumns(String db, String table, String[] cols)
	{
		String rec =  "{" + col_cmd + "=" + command.addFulltextColumn.getByte() + ","
							+ col_param + "=" + db + param_delim + table + param_delim
							+ patchParams(cols)
							+ "}";
					
		return rec;
		
	}
	
	public static void testPatchingParams()
	{
		String[] params = new String[3];
		params[0] = "p1";
		params[1] = "p2";
		params[2] = "p3";
		System.out.println(LogCMDConstructor.patchParams(params));
	}
	
	public static void testConstructLogAddingFulltextColumns()
	{
		String[] cols = new String[2];
		cols[0] = "col1";
		cols[1] = "col2";
		System.out.println(contructLogAddingFulltextColumns("db1", "table2",cols));
	}
	
	public static void main(String[] args)
	{
		//testPatchingParams();
		testConstructLogAddingFulltextColumns();
		
	}
	
}
