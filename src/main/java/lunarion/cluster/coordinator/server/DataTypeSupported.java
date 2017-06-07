
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
package lunarion.cluster.coordinator.server;

import LCG.MemoryIndex.IndexTypes;
import LCG.MemoryIndex.IndexTypes.DataTypes;

public class DataTypeSupported {

	/*
	 * @IndexTypes.DataTypes
	 */
	
	public static boolean supported(String type_in_string)
	{
		return DataTypes.UNKNOWN != IndexTypes.getDatatype(type_in_string);
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
