
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
 

import java.util.ArrayList;

import LCG.EnginEvent.Event;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import io.netty.buffer.ByteBuf;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command;

public class MessageResponseForQuery extends MessageResponse{
	 
	 
	
	private void setIfNoRecs(String db, String table)
	{
		params_in_bytes = new ArrayList<byte[]>(3); 
		
		params_in_bytes.add(VariableGeneric.utf8Encode(db,0, db.length())) ;
		params_in_bytes.add(VariableGeneric.utf8Encode(table,0, table.length())) ;  
		params_in_bytes.add(VariableGeneric.utf8Encode(this.null_str,0, this.null_str.length())) ;  
		
		/*
		size_of_recs += (params_in_bytes.get(0).length + this.delim.length() 
							+ params_in_bytes.get(1).length + this.delim.length() 
							+ params_in_bytes.get(2).length);
							*/
		size_of_recs += (this.byte_for_length + params_in_bytes.get(0).length 
							+ this.byte_for_length + params_in_bytes.get(1).length 
							+ this.byte_for_length + params_in_bytes.get(2).length);
	}
	 
	@Override
	public void setParamsFromNode(String db, String table, ArrayList<Record32KBytes> _recs)
	{
		if(_recs == null || _recs.size()==0 )
		{ 
			setIfNoRecs( db, table) ;
			return ;
		}
		this.params_in_bytes = new ArrayList<byte[]>( _recs.size() + 2);
		params_in_bytes.add(VariableGeneric.utf8Encode(db,0, db.length())) ;
		params_in_bytes.add(VariableGeneric.utf8Encode(table,0, table.length())) ;  
		
		size_of_recs += (this.byte_for_length +  params_in_bytes.get(0).length 
							+ this.byte_for_length +  params_in_bytes.get(1).length );  
		  
		for(int i = 0; i < _recs.size(); i++)
		{
			byte[] rec_i_in_byte  = null;
			if(_recs.get(i)!= null)
			{  
				String rec = _recs.get(i).recData();
				rec_i_in_byte = VariableGeneric.utf8Encode(rec,0,rec.length()); 
			}
			else
			{
				rec_i_in_byte = VariableGeneric.utf8Encode(this.null_str,0, this.null_str.length()); 
			}
			
			params_in_bytes.add(rec_i_in_byte);
			 
			size_of_recs +=  this.byte_for_length + rec_i_in_byte.length ;  
		 
		} 
	} 
	
	@Deprecated
	public void setParamsFromNodeDeprecated(String db, String table, ArrayList<Record32KBytes> _recs)
	{
		if(_recs == null || _recs.size()==0 )
		{ 
			setIfNoRecs( db, table) ;
			return ;
		}
		this.params_in_bytes = new ArrayList<byte[]>( _recs.size() + 2);
		params_in_bytes.add(VariableGeneric.utf8Encode(db,0, db.length())) ;
		params_in_bytes.add(VariableGeneric.utf8Encode(table,0, table.length())) ;  
		
		size_of_recs += (params_in_bytes.get(0).length + this.delim.length() 
							+ params_in_bytes.get(1).length + this.delim.length() );  
		  
		for(int i = 0; i < _recs.size()-1; i++)
		{
			byte[] rec_i_in_byte  = null;
			if(_recs.get(i ) != null)
			{
				String rec_i = _recs.get(i ).recData(); 
				rec_i_in_byte = VariableGeneric.utf8Encode(rec_i,0, rec_i.length()); 
			}
			else
			{
				rec_i_in_byte = VariableGeneric.utf8Encode(this.null_str,0, this.null_str.length());
				 
			}
			
			params_in_bytes.add(rec_i_in_byte);
			size_of_recs += rec_i_in_byte.length + this.delim.length()  ;  
		 
		} 
		
		if(_recs.get(_recs.size()-1 )  != null)
		{
			byte[] rec_i_in_byte  = null;
			String rec_i = _recs.get(_recs.size()-1 ).recData(); 
			rec_i_in_byte = VariableGeneric.utf8Encode(rec_i,0, rec_i.length());
			params_in_bytes.add(rec_i_in_byte);
			 
			size_of_recs += rec_i_in_byte.length;  
		
		}
		else
		{
			String rec_i = this.null_str; 
			byte[] rec_i_in_byte = VariableGeneric.utf8Encode(rec_i, 0, rec_i.length());
			params_in_bytes.add(rec_i_in_byte);
			 
			size_of_recs += rec_i_in_byte.length;  
		}
		
		
	} 
	
	public void setParams(String db, String table,  ArrayList<String> _recs)
	{
		if(_recs == null || _recs.size()==0 )
		{ 
			setIfNoRecs( db, table) ;
			return ;
		} 
		 
		this.params_in_bytes = new ArrayList<byte[]>( _recs.size() + 2);
		params_in_bytes.add(VariableGeneric.utf8Encode(db,0, db.length())) ;
		params_in_bytes.add(VariableGeneric.utf8Encode(table,0, table.length())) ;  
		
		size_of_recs += (this.byte_for_length +  params_in_bytes.get(0).length 
							+ this.byte_for_length +  params_in_bytes.get(1).length );  
		  
		for(int i = 0; i < _recs.size() ; i++)
		{
			byte[] rec_i_in_byte  = null;
			if(_recs.get(i ) != null)
			{
				String rec_i = _recs.get(i ) ; 
				rec_i_in_byte = VariableGeneric.utf8Encode(rec_i,0, rec_i.length()); 
			}
			else
			{
				rec_i_in_byte = VariableGeneric.utf8Encode(this.null_str,0, this.null_str.length());
				 
			}
			
			params_in_bytes.add(rec_i_in_byte);
			size_of_recs +=  this.byte_for_length + rec_i_in_byte.length ;  
		 
		} 
		
	} 
	

	
	@Deprecated
	public void setParamsFromCoordinator(String db, String table,  ArrayList<String> _recs)
	{
		if(_recs == null || _recs.size()==0 )
		{ 
			setIfNoRecs( db, table) ;
			return ;
		} 
		 
		this.params_in_bytes = new ArrayList<byte[]>( _recs.size() + 2);
		params_in_bytes.add(VariableGeneric.utf8Encode(db,0, db.length())) ;
		params_in_bytes.add(VariableGeneric.utf8Encode(table,0, table.length())) ;  
		
		size_of_recs += (params_in_bytes.get(0).length + this.delim.length() 
							+ params_in_bytes.get(1).length + this.delim.length() );  
		  
		for(int i = 0; i < _recs.size()-1; i++)
		{
			byte[] rec_i_in_byte  = null;
			if(_recs.get(i ) != null)
			{
				String rec_i = _recs.get(i ) ; 
				rec_i_in_byte = VariableGeneric.utf8Encode(rec_i,0, rec_i.length()); 
			}
			else
			{
				rec_i_in_byte = VariableGeneric.utf8Encode(this.null_str,0, this.null_str.length());
				 
			}
			
			params_in_bytes.add(rec_i_in_byte);
			size_of_recs += rec_i_in_byte.length + this.delim.length()  ;  
		 
		} 
		
		if(_recs.get(_recs.size()-1 )  != null)
		{
			byte[] rec_i_in_byte  = null;
			String rec_i = _recs.get(_recs.size()-1 ) ; 
			rec_i_in_byte = VariableGeneric.utf8Encode(rec_i,0, rec_i.length());
			params_in_bytes.add(rec_i_in_byte);
			 
			size_of_recs += rec_i_in_byte.length;  
		
		}
		else
		{
			String rec_i = this.null_str; 
			byte[] rec_i_in_byte = VariableGeneric.utf8Encode(rec_i, 0, rec_i.length());
			params_in_bytes.add(rec_i_in_byte);
			 
			size_of_recs += rec_i_in_byte.length;  
		}
		
	} 
	

	
	
	
}
