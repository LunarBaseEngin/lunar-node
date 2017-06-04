
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
	static public String add_storable_column_succeed = "[SUCEED 00000004]: storable column has been sucessfully added.";
	static public String add_analytic_column_succeed = "[SUCEED 00000005]: analytic column has been sucessfully added.";
	static public String insert_succeed = "[SUCEED 00000006]: successfully inserted all the records.";
	static public String result_removed_succeed = "[SUCEED 00000007]: successfully removed query result in cache.";
	static public String add_column_succeed = "[SUCEED 00000008]: add column succeed for analytic/fulltextSearch/Storable.";
	
	
	
	static public String wrong_parameter_count = "[FAILURE 00000000]: the number of parameters are wrong.";
	
	static public String create_table_failed_already_exists = "[FAILURE 00000001]: table already exists, can not be created again.";
	static public String create_table_failed_exception = "[FAILURE 00000002]: exception occurred when creating the table.";
	static public String add_fulltext_column_failed = "[FAILURE 00000003]: full text search column failed adding.";
	static public String table_does_not_exist = "[FAILURE 00000004]: table does not exist.";
	static public String db_does_not_exist = "[FAILURE 00000005]: database does not exist.";
	static public String create_log_table_failed_exception = "[FAILURE 00000006]: exception occurred when creating the log table.";
	static public String no_records_found = "[FAILURE 00000007]: found no records in the table.";
	
	static public String wrong_parameters_for_feteching_name_with_suffix = "[FAILURE 00000008]: wrong parameters for fetching table names with given suffix.";
	static public String no_table_found = "[FAILURE 00000009]: no table found.";
	static public String empty_name = "[FAILURE 00000010]: db name or table name can not be empty, failt to create table.";
	static public String illegal_table_name = "[FAILURE 00000011]: can not create table with illegal name. The name you commit may end with _log. remove it.";
	
	static public String wrong_parameters_for_notifying_update = "[FAILURE 00000012]: can not nofity slaves any updates, notifySlavesUpdate needs at least 2 parameters: db name, table name who has been updated.";
	static public String does_not_has_null_result_uuid = "[FAILURE 000000013]: does not has query result for a null result uuid.";
	static public String io_exception_in_fetching_records = "[FAILURE 000000014]: IO exception occurs in fetching records.";
	static public String no_column_found = "[FAILURE 000000015]: no columns found.";
	
	static public String add_storable_column_failed = "[FAILURE 000000016]: storable column failed adding.";
	static public String add_analytic_column_failed = "[FAILURE 000000017]: analytic column failed adding.";
	
	static public String wrong_parameters_for_sql_filter = "[FAILURE 00000018]: wrong parameters for sql filter, which requires atl east 3 parameters: db, table, statement known to LunarDB.";
	static public String wrong_parameters_for_records_count = "[FAILURE 00000019]: wrong parameters for fetching records count.";
	
	static public String result_removed_failed = "[FAILURE 00000020]: fail in removing query result in cache.";
	static public String add_column_failed = "[FAILURE 00000021]: fail to add column for analytic/fulltextSearch/Storable.";
	
	static public String rec_id_out_of_boundary = "[FAILURE 00000022]: one node does not support records more than the maximum value of integer.";
	
	static public String unknown_cmd = "[FAILURE 00000023]: the command is unknown to lunar-node .";
	
}
