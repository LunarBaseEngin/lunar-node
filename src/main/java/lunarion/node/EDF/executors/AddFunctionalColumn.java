
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

import java.io.IOException;

import org.apache.log4j.Logger;

import LCG.DB.API.LunarDB;
import LCG.DB.API.LunarTable;
import LCG.DB.Local.NLP.FullText.Lexer.TokenizerForSearchEngine;
import LCG.MemoryIndex.IndexTypes;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command;
import lunarion.node.LunarDBServerStandAlone;
import lunarion.node.EDF.ExecutorInterface;
import lunarion.node.logger.TableOperationLogger;
import lunarion.node.logger.Timer;
import lunarion.node.remote.protocol.CodeSucceed;
import lunarion.node.remote.protocol.MessageRequest;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.utile.ControllerConstants;

public class AddFunctionalColumn implements ExecutorInterface{

	CMDEnumeration.command column_purpose;
	
	public AddFunctionalColumn(CMDEnumeration.command  _column_purpose)
	{
		column_purpose = _column_purpose;
	}
	
	public MessageResponse execute(LunarDBServerStandAlone l_db_ssa , MessageRequest msg, Logger logger)
	{
		 CMDEnumeration.command cmd = msg.getCMD();
		 
		
		 return addFunctionalColumn(l_db_ssa, msg, logger, column_purpose);
	}
	
	/*
     * @param  
     * params[0]: db name;
     * params[1]: table name;
     * params[2]: column name;
     * params[3]: column type, for command.addAnalyticColumn;
     * 
     * Response:
     * resp[0] = db;
     * resp[1] = table;
     * resp[2] = CodeSucceed.create_table_succeed, or failure message.
     * resp[3] = CodeSucceed. add  column succeed, or failure message.
     */
    protected MessageResponse addFunctionalColumn(LunarDBServerStandAlone l_db_ssa , 
    												MessageRequest request, 
    												Logger logger,
    												CMDEnumeration.command  _column_purpose)
    {
    	String[] params = (String[])request.getParams();
    	 
     	MessageResponse response = null;
     	
    	boolean suc = true;
    	if(_column_purpose == command.addAnalyticColumn && params.length < 4)
    	{
    		System.err.println("[NODE ERROR]: addAnalyticColumn needs at least 4 parameters: db name, table name, column name and column type");
			logger.info("[NODE ERROR]: addAnalyticColumn needs at least 4 parameters: db name, table name, column name and column type");
			
			suc = false ;
			response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(),CodeSucceed.wrong_parameter_count);
			return response;
    	}
		if(params.length < 3 )
		{
			System.err.println("[NODE ERROR]: addFulltextColumn needs at least 3 parameters: db name, table name and column name");
			logger.info("[NODE ERROR]: addFulltextColumn needs at least 3 parameters: db name, table name and column name");
			
			suc = false ;
			response = ExecutorInterface.responseError(request, MessageResponse.getNullStr(), MessageResponse.getNullStr(),CodeSucceed.wrong_parameter_count);
			return response;
		}
		
		String db = params[0];
		String table = params[table_name_index];
		String log_table = ControllerConstants.getLogTableName(table);
		String column = params[2];
		String column_type = "";
		if(_column_purpose == command.addAnalyticColumn)
			column_type = params[3];
		
		if(db == null || "".equals(db.trim()) 
				||table == null || "".equals(table.trim()))
			suc = false ;
		
		String[] resp = new String[4];
		resp[0] = db;
		resp[table_name_index] = table;
		  	
		
        LunarDB l_DB = l_db_ssa.getDBInstant(db);
        if(l_DB == null)
        {
        	suc = false;
        	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.db_does_not_exist);
        	return response;
        }
        else
        {
        	if(suc && !l_DB.hasTable(table))
            {	
            	suc = l_DB.createTable(table);
            	l_DB.openTable(table);
            	if(suc)
            	{
            		resp[3] = CodeSucceed.create_table_succeed;
            		logger.info(Timer.currentTime() + " " + CodeSucceed.create_table_succeed); 
            	}
            	else
            	{
            		resp[3] = CodeSucceed.create_table_failed_exception;
            		logger.info(Timer.currentTime() + " " + CodeSucceed.create_table_failed_exception); 
            	}
            	
            	
            	
            	boolean log_table_created = false;
            	log_table_created = l_DB.createTable(log_table);
            	l_DB.openTable(log_table);
            	if(log_table_created)
            	{
            		resp[2] = CodeSucceed.create_log_table_succeed;
            		logger.info(Timer.currentTime() + " " + CodeSucceed.create_log_table_succeed); 
            	}
            	else
            	{
            		resp[2] = CodeSucceed.create_log_table_failed_exception;
            		logger.info(Timer.currentTime() + " " + CodeSucceed.create_log_table_failed_exception); 
            	}
            	
            	TableOperationLogger.logCreatingTable( db, table, log_table, l_DB);
            	
            }
            if(suc)
            {
            	LunarTable tt = l_DB.getTable(table); 
            	switch(_column_purpose)
            	{
            	case addAnalyticColumn:
	            	{
	            		boolean ok = false;
	            		
	            		try {
	            			if(column_type.equalsIgnoreCase(IndexTypes.getTypeString(IndexTypes.DataTypes.STRING))
	            					|| column_type.equalsIgnoreCase(IndexTypes.getTypeString(IndexTypes.DataTypes.TEXT)))
	            			{ 
	            				ok = tt.addSearchable(column_type, column);
		            			
	            				TokenizerForSearchEngine t_e = new TokenizerForSearchEngine(); 
	    	            		tt.registerTokenizer(column, t_e); 
	            			} 
	            			else
	            				ok = tt.addSearchable(column_type, column);
	            			
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} 
	        			 
	            		if(ok)
	            		{
								resp[2] = CodeSucceed.add_analytic_column_succeed;
	            		}
	            		else
	            		{
								resp[2] = CodeSucceed.add_analytic_column_failed;
								suc = false;
	            		} 
	            		
	            		//TODO: log this when i have time
	        		}
            		break;
            	case addStorableColumn:
            		{
            			boolean ok = false;
	            		try {
							ok = tt.addStorable(column);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	            		
	            		if(ok)
						{
							resp[2] = CodeSucceed.add_storable_column_succeed;
						}
						else
						{
							resp[2] = CodeSucceed.add_storable_column_failed;
							suc = false;
						}  
	            		
	            		//TODO: log this when i have time
            		}
            		break;
            	case addFulltextColumn:
	            	{
	            		if(tt.addFulltextSearchable(column))
	             		{
	             			resp[2] = CodeSucceed.add_fulltext_column_succeed;
	             		}
	             		else
	             		{
	             			resp[2] = CodeSucceed.add_fulltext_column_failed;
	             			suc = false;
	             		}
	             		 
	            		TokenizerForSearchEngine t_e = new TokenizerForSearchEngine(); 
	            		tt.registerTokenizer(column, t_e);
	                    
	            		TableOperationLogger.logAddingFulltextColumn( db, table, column, l_DB);
	            	}
            		break; 
            	}
         		
        	    
            }
            else
            {
            	response = ExecutorInterface.responseError(request, db, table, CodeSucceed.add_column_failed);
            	return response; 
            }
        } 
        
        response = new MessageResponse();
        response.setUUID(request.getUUID());
        response.setCMD(request.getCMD());
        response.setSucceed(suc); 
        response.setParams(resp);  
        
        logger.info(Timer.currentTime() + " " + CodeSucceed.add_column_succeed); 
        
        return response;
    }
}
