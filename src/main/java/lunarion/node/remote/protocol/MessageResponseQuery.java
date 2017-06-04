
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

public class MessageResponseQuery extends MessageResponse{
	 
	@Override
	public void setParamsFromNode(String db, String table, ArrayList<Record32KBytes> _params)
	{
		if(_params == null || _params.size()==0 )
		{
			this.params = new String[3];
			this.params[0] = db;
			this.params[1] = table;
			this.params[2] = null;
			
			return ;
		}
		this.params = new String[_params.size()+2];
		this.params[0] = db;
		this.params[1] = table;
		
		for(int i = 2; i < this.params.length; i++)
		{
			this.params[i] = _params.get(i-2).recData() ;
		} 
	} 
	
	public void setParamsFromCoordinator(String db, String table,  ArrayList<String> _params)
	{
		if(_params == null || _params.size()==0 )
		{
			this.params = new String[3];
			this.params[0] = db;
			this.params[1] = table;
			this.params[2] = null;
			
			return ;
		}
		this.params = new String[_params.size()+2];
		this.params[0] = db;
		this.params[1] = table;
		
		for(int i = 2; i < this.params.length; i++)
		{
			this.params[i] = _params.get(i-2) ;
		} 
	} 
}
