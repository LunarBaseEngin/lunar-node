
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
import java.util.StringTokenizer;

import LCG.EnginEvent.Event;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import io.netty.buffer.ByteBuf;
import lunarion.db.local.shell.CMDEnumeration;

public class MessageRequest extends MessageToWrite{ 
	
	protected String[] params; //for reading
	 
	
	public MessageRequest()
	{
	}  
	  
	public String[] getParams()
	{
		return this.params;
	}
	
	
	public void read(ByteBuf message_byte_buf) throws UnsupportedEncodingException  
	{  
		cmd = CMDEnumeration.getCMD(message_byte_buf.readByte()); 
		succeed = message_byte_buf.readByte()==0?false:true; 
		
		byte[] uuid_len_bytes = new byte[4];
		message_byte_buf.readBytes(uuid_len_bytes);
		int uuid_len = VariableGeneric.Transform4ByteToInt(uuid_len_bytes, 0);
	 
		byte[] uuid_in_bytes = new byte[uuid_len];
		message_byte_buf.readBytes(uuid_in_bytes);
		 
		message_uuid = new String(uuid_in_bytes, 0, uuid_in_bytes.length, "utf-8"); 
	 	 
		byte[] count_len_bytes = new byte[4];
		message_byte_buf.readBytes(count_len_bytes);
		int count =  VariableGeneric.Transform4ByteToInt(count_len_bytes, 0);
		this.params = new String[count]; 
	 
		for(int i=0;i<this.params.length;i++)
		{ 
			params[i] = transformBytesToUTF8String(readOneParamInBuff( message_byte_buf));  
		}
		 
	}   
	
	@Deprecated
	public void readDeprecated(ByteBuf message_byte_buf)
	{ 
		byte[] raw_byte_buf = new byte[message_byte_buf.readableBytes()];
		message_byte_buf.readBytes(raw_byte_buf);
	 
		 
		cmd = CMDEnumeration.getCMD(raw_byte_buf[0]); 
		succeed = raw_byte_buf[1]==0?false:true;
		 
		 
		String str = "";
		try {
			str = new String(raw_byte_buf, 2, raw_byte_buf.length-2, "utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 	StringTokenizer stringTokenizer = new StringTokenizer(str, delim);   
	 
		if(stringTokenizer.hasMoreElements())  
			message_uuid =  (String) stringTokenizer.nextElement(); 
	 
		this.params = new String[stringTokenizer.countTokens() ];
		
		 
		for(int i=0;i<this.params.length;i++)
		{  
			this.params[i] =  (String) stringTokenizer.nextElement();
		} 
	}   
}
