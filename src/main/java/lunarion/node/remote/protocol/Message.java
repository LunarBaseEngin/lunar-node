
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

import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import io.netty.buffer.ByteBuf;
import lunarion.db.local.shell.CMDEnumeration;

public class Message extends Object{
	
	protected final int cmd_at = 0;//1 byte, 256 commands
	protected CMDEnumeration.command cmd;
	protected boolean succeed=false;
	
	protected static String delim = ReservedSymbols.remote_message_delim;
		
	protected static String null_str = "null"; 
	protected String message_uuid;
	protected String[] params;
	
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

	public void setParams(String[] _params)
	{
		this.params = _params;
	}
	
	
	public String[] getParams()
	{
		return this.params;
	}
	public String getUUID()
	{
		return this.message_uuid;
	}
	
	public String setUUID(String uuid)
	{
		return this.message_uuid = uuid;
	}
	
	public void write(ByteBuf message_byte_buf) 
	{
		message_byte_buf.writeByte(cmd.getByte());
		byte s =(byte) (succeed?1:0);
		message_byte_buf.writeByte(s);
		int count = this.params.length;
	 
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
			if(this.params[i]!=null)
			{
				message_byte_buf.writeBytes(
						VariableGeneric.utf8Encode(
								this.params[i],0,this.params[i].length())
						);  
			}
			else
			{
				 
				message_byte_buf.writeBytes(
						VariableGeneric.utf8Encode(
								this.null_str,0,this.null_str.length())
						);
						 
			}
			message_byte_buf.writeBytes(
					VariableGeneric.utf8Encode(
							delim,0,delim.length())
					);
			
			
		}
		if(this.params[count-1]!=null)
		{
			message_byte_buf.writeBytes(
				VariableGeneric.utf8Encode(
						this.params[count-1],0,this.params[count-1].length())
				);
		}
		else
		{
			message_byte_buf.writeBytes(
					VariableGeneric.utf8Encode(
							this.null_str,0,this.null_str.length())
					);
		}
	}
	
	public int size()
	{
		int size = 1+1 + message_uuid.length() + this.delim.length();
		for(int i=0;i<this.params.length-1;i++)
		{
			int len = 0;
			if(params[i]!=null)
			{
				len = VariableGeneric.utf8Encode(params[i],0, params[i].length()).length 
						+  VariableGeneric.utf8Encode( this.delim,0,  this.delim.length()).length ;
			}
			else
			{
				len = VariableGeneric.utf8Encode(this.null_str,0, this.null_str.length()).length 
						+  VariableGeneric.utf8Encode( this.delim,0,  this.delim.length()).length ;
			}
			size += len;
		}
		if(params[this.params.length-1] != null)
			size += VariableGeneric.utf8Encode(params[this.params.length-1],0, params[this.params.length-1].length()).length;  
		else
			size += VariableGeneric.utf8Encode(this.null_str,0,this.null_str.length()).length;  
		//size += params[this.params.length-1].length();
		
		return size;
	}
}
