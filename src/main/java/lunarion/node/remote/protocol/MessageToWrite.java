
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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import io.netty.buffer.ByteBuf;
import lunarion.db.local.shell.CMDEnumeration;

public class MessageToWrite extends Object{
	
	protected final int cmd_at = 0;//1 byte, 256 commands
	protected CMDEnumeration.command cmd;
	protected boolean succeed=false;
	
	//@Deprecated
	protected static String delim = ReservedSymbols.remote_message_delim;
	protected int byte_for_length = 4;
	 
	
	protected static String null_str = "null"; 
	protected String message_uuid;
	int size_of_recs = 0;
	
	ArrayList<byte[]> params_in_bytes = null;  //for writing
	
	public static String getNullStr()
	{
		return null_str;
	}	
	
	public CMDEnumeration.command getCMD()
	{ 
		return cmd;
	}
	
	public void setCMD(CMDEnumeration.command _cmd)
	{
		this.cmd = _cmd;
	}
	 
	
	public void setSucceed(boolean if_succeed)
	{
		this.succeed = if_succeed;
	}
		
	public boolean isSucceed()
	{
		return this.succeed;
	}

	public void setParams(String[] params)
	{
		this.params_in_bytes = new ArrayList<byte[]>(params.length);
		 
		for(int i = 0; i < params.length ; i++)
		{
			byte[] rec_i_in_byte  = null;
			if(params[i]!= null)
			{  
				rec_i_in_byte = VariableGeneric.utf8Encode(params[i],0, params[i].length()); 
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
	public void setParamsDeprecated(String[] params)
	{
		this.params_in_bytes = new ArrayList<byte[]>(params.length);
		 
		for(int i = 0; i < params.length-1; i++)
		{
			byte[] rec_i_in_byte  = null;
			if(params[i]!= null)
			{  
				rec_i_in_byte = VariableGeneric.utf8Encode(params[i],0, params[i].length()); 
			}
			else
			{
				rec_i_in_byte = VariableGeneric.utf8Encode(this.null_str,0, this.null_str.length()); 
			}
			
			params_in_bytes.add(rec_i_in_byte);
			size_of_recs += rec_i_in_byte.length + this.delim.length()  ;  
			 
		} 
		
		if(params[params.length-1 ] != null)
		{  
			String rec_i = params[params.length-1 ]; 
			byte[] rec_i_in_byte = VariableGeneric.utf8Encode(rec_i, 0, rec_i.length());
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
	
	public String getUUID()
	{
		return this.message_uuid;
	}
	
	public String setUUID(String uuid)
	{
		return this.message_uuid = uuid;
	}
	
	protected String transformBytesToUTF8String(byte[] str_in_bytes) throws UnsupportedEncodingException
	{
		return new String(str_in_bytes, 0, str_in_bytes.length, "utf-8"); 
	}
	protected byte[] readOneParamInBuff(ByteBuf message_byte_buf)  
	{
		byte[] param_len_in_bytes = new byte[4];
		message_byte_buf.readBytes(param_len_in_bytes);
		int param_len = VariableGeneric.Transform4ByteToInt(param_len_in_bytes, 0);
		byte[] param_in_bytes = new byte[param_len];
		message_byte_buf.readBytes(param_in_bytes);
	  
		return param_in_bytes ; 
	}
	
	public void write(ByteBuf message_byte_buf) 
	{
		message_byte_buf.writeByte(cmd.getByte());
		byte s =(byte) (succeed?1:0);
		message_byte_buf.writeByte(s);
		int count = this.params_in_bytes.size();
	 
		byte[] uuid = VariableGeneric.utf8Encode(
				this.message_uuid,0,this.message_uuid.length());
		byte[] uuid_len = new byte[4];
		VariableGeneric.TransformIntTo4Byte(uuid_len, 0, this.message_uuid.length());
		message_byte_buf.writeBytes(uuid_len); 
		message_byte_buf.writeBytes(uuid); 
	 
		byte[] param_count_in_bytes = new byte[4];
		VariableGeneric.TransformIntTo4Byte(param_count_in_bytes, 0, count);
		
		message_byte_buf.writeBytes(param_count_in_bytes); 
		
		
		for(int i=0; i<count ;i++)
		{ 
			byte[] param_len = new byte[4];
			VariableGeneric.TransformIntTo4Byte(param_len, 0, this.params_in_bytes.get(i).length);
			message_byte_buf.writeBytes(param_len);  
			message_byte_buf.writeBytes(this.params_in_bytes.get(i));   
		} 
	}
	
	@Deprecated
	public void writeDeprecated(ByteBuf message_byte_buf) 
	{
		message_byte_buf.writeByte(cmd.getByte());
		byte s =(byte) (succeed?1:0);
		message_byte_buf.writeByte(s);
		int count = this.params_in_bytes.size();
	 
		message_byte_buf.writeBytes(
				VariableGeneric.utf8Encode(
						this.message_uuid,0,this.message_uuid.length())
				);
		message_byte_buf.writeBytes(
				VariableGeneric.utf8Encode(
						delim,0,delim.length())
				);
		for(int i=0; i<count-1;i++)
		{ 
			message_byte_buf.writeBytes(this.params_in_bytes.get(i));  
			 
			message_byte_buf.writeBytes(
										VariableGeneric.utf8Encode(
												delim,0,delim.length())
										); 
		}
		 
		message_byte_buf.writeBytes(this.params_in_bytes.get(count-1));
		 
		 
	}
	
	@Deprecated 
	public int sizeDeprecated()
	{
		int size = 1+1 + message_uuid.length() + this.delim.length();
		 
		size +=	size_of_recs;
		
		return size;
	}
	public int size()
	{
		/*
		 * cmd, succeed, 4, uuid, count of params
		 */
		int size = 1+1 + byte_for_length + message_uuid.length() + 4;
		 
		size +=	size_of_recs;
		
		return size;
	}
}
