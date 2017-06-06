
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

import LCG.EnginEvent.Event;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import LCG.StorageEngin.Serializable.Impl.VariableGeneric;
import io.netty.buffer.ByteBuf;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.db.local.shell.CMDEnumeration.command;

public class MessageResponseForDriver extends MessageResponseQuery {
	 
	@Override
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
			 
				db_name = all_params[1];
				table_name = all_params[2];
				this.params = new String[all_params.length-3];
				//System.arraycopy(all_params, 1, this.params, 0, all_params.length-1);
				for(int i=0;i<this.params.length;i++)
				{
					if(!all_params[i+3].equalsIgnoreCase(this.null_str))
						this.params[i] = all_params[i+3];
					else
						this.params[i] = null;
				}
				return;
		 
			
		}
		 
		this.params = new String[all_params.length-1];
		 
		for(int i=0;i<this.params.length;i++)
		{
			if(!all_params[i+1].equalsIgnoreCase(this.null_str))
				this.params[i] = all_params[i+1];
			else
				this.params[i] = null;
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
