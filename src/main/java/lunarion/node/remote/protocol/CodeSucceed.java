
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

public class CodeSucceed {
	static public String create_table_succeed = "[SUCEED 00000001]: table has been sucessfully created.";
	static public String add_fulltext_column_succeed = "[SUCEED 00000002]: full text search column has been sucessfully added.";
	static public String create_log_table_succeed = "[SUCEED 00000003]: log table has been sucessfully created.";
	
	static public String wrong_parameters = "[FAILURE 00000000]: parameters are wrong.";
	
	static public String create_table_failed_already_exists = "[FAILURE 00000001]: table already exists, can not be created again.";
	static public String create_table_failed_exception = "[FAILURE 00000002]: exception occurred when creating the table.";
	static public String add_fulltext_failed = "[FAILURE 00000003]: full text search column failed adding.";
	static public String table_does_not_exist = "[FAILURE 00000004]: table does not exist.";
	static public String db_does_not_exist = "[FAILURE 00000005]: database does not exist.";
	static public String create_log_table_failed_exception = "[FAILURE 00000006]: exception occurred when creating the log table.";
	static public String no_records_found = "[FAILURE 00000007]: found no records in the table.";
	
}
