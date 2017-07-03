
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

import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import io.netty.buffer.ByteBuf;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command;

public class MessageResponseForDriver extends MessageResponseForQuery {
	 
	@Override
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
		
		
		if(cmd == command.fetchQueryResultRecs 
				|| cmd == command.fetchRecordsASC 
				|| cmd == command.fetchRecordsDESC)
		{
			/*
			 * all_params[0]: uuid
			 * all_params[1]: db_name
			 * all_params[2]: table_name
			 * 
			 * ignore the first three params 
			 */
			if(count < 2)
				return;
			db_name = transformBytesToUTF8String(readOneParamInBuff( message_byte_buf)) ;
			table_name = transformBytesToUTF8String(readOneParamInBuff( message_byte_buf)) ;
			
			if(count == 2)
				return;
			
			this.params = new String[count - 2];
			for(int i=0;i<this.params.length;i++)
			{ 
				params[i] = transformBytesToUTF8String(readOneParamInBuff( message_byte_buf)) ;
				 
			} 
			return;  
			
		}
		 
		this.params = new String[count];
		 
		for(int i=0;i<this.params.length;i++)
		{ 
			this.params[i] =  transformBytesToUTF8String(readOneParamInBuff( message_byte_buf));
		} 
		 
		if(cmd == command.getColumns ||cmd == command.closeQueryResult )
		{
				db_name = "";
				table_name = "";
				return;
		}
			 
		db_name =  this.params[0];
		table_name = this.params[1];
		 
	}
	 
}
