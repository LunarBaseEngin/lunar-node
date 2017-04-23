
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

import LCG.EnginEvent.Event;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import io.netty.buffer.ByteBuf;
import lunarion.db.local.shell.CMDEnumeration;

public class MessageRequest extends Event{
	 
	//private byte[] raw_byte_buf; 
	private final int cmd_at = 0;//1 byte, 256 commands
	private CMDEnumeration.command cmd;
	private boolean succeed=false;
	
	private static String delim = ReservedSymbols.remote_message_delim;
	private String null_str = "null"; 
	private String message_uuid;
	String[] params;
	
	public MessageRequest()
	{
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
	
	public void read(ByteBuf message_byte_buf)
	{ 
		byte[] raw_byte_buf = new byte[message_byte_buf.readableBytes()];
		message_byte_buf.readBytes(raw_byte_buf);
	 
		
		cmd = CMDEnumeration.getCMD(raw_byte_buf[0]);
		//cmd = CMDEnumeration.getCMD(message_byte_buf.getByte(0));
		succeed = raw_byte_buf[1]==0?false:true;
		//succeed = message_byte_buf.getByte(1)==0?false:true;
		
		 
		String str = "";
		try {
			str = new String(raw_byte_buf, 2, raw_byte_buf.length-2, "utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//String str = new String(message_byte_buf.array(), 2, message_byte_buf.readableBytes() -2 );
		String[] all_params = str.split(delim);
		message_uuid = all_params[0];
		this.params = new String[all_params.length-1];
		//System.arraycopy(all_params, 1, this.params, 0, all_params.length-1);
		for(int i=0;i<this.params.length;i++)
		{
			if(!all_params[i+1].equalsIgnoreCase(this.null_str))
				this.params[i] = all_params[i+1];
			else
				this.params[i] = null;
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
