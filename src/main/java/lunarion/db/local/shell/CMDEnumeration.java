package lunarion.db.local.shell;

import LCG.DB.Grammar.Parser.QueryToken;

public class CMDEnumeration {
	public static enum entry_command {
		lunardb, wait, doit, quit
	};

	public static enum command {
		
		start{
			public byte getByte(){return 0;}  
		},
		stop{
			public byte getByte(){return 1;}  
		}, 
		createDB{
			public byte getByte(){return 2;}  
		}, 
		createTable{
			public byte getByte(){return 3;}  
		}, 
		openDB{
			public byte getByte(){return 4;}  
		}, 
		closeDB{
			public byte getByte(){return 5;}  
		}, 
		insert{
			public byte getByte(){return 6;}  
		}, 
		delete{
			public byte getByte(){return 7;}  
		},
		/* full text query*/
		ftQuery{
			public byte getByte(){return 8;}  
		}, 
		/* point query, e.g. where price=100; */
		ptQuery{
			public byte getByte(){return 9;}  
		}, 
		/* range query, e.g. where price>100 and price <200 or 50<distance and distance <=100 */
		rgQuery{
			public byte getByte(){return 10;}  
		},  
		update{
			public byte getByte(){return 11;}  
		}, 
		
		/*
		 * restart when cache size is changed
		 */
		restart{
			public byte getByte(){return 12;}  
		}, 
		/*
		 * rebuild the whole database from raw data when the following core
		 * specifications are changed: manifold size, block level, hash
		 * capacity. Or when the database is damaged by whatever outer forces.
		 * 
		 */
		rebuild{
			public byte getByte(){return 13;}  
		},
		/*
		 * migrate when you want to merge the records table files into a one big
		 * file, or scatter them to more smaller pieces. Record ID keep
		 * unchanged. If you reorder theses records, you must call rebuild
		 * immediately before lunarDB provides its service.
		 */
		migrant{
			public byte getByte(){return 14;}  
		},
		status{
			public byte getByte(){return 15;}  
		},  
		version{
			public byte getByte(){return 16;}  
		},
		backup{
			public byte getByte(){return 17;}  
		},
		addFulltextColumn{
			public byte getByte(){return 18;}  
		},
		fetchRecordsDESC{
			public byte getByte(){return 19;}  
		}, 
		fetchRecordsASC{
			public byte getByte(){return 20;}  
		}, 
		fetchLog{
			public byte getByte(){return 21;}  
		},
		fetchTableNamesWithSuffix{
			public byte getByte(){return 22;}  
		},
		notifySlavesUpdate{
			public byte getByte(){return 23;}  
		},
		fetchQueryResultRecs{
			public byte getByte(){return 24;}  
		},
		addAnalyticColumn{
			public byte getByte(){return 25;}  
		},
		addStorableColumn{
			public byte getByte(){return 26;}   
		},
		filterForWhereClause{
			public byte getByte(){return 27;}   
		},
		getColumns{
			public byte getByte(){return 28;}   
		},
		recsCount{
			public byte getByte(){return 29;}   
		},
		closeQueryResult{
			public byte getByte(){return 30;}   
		},
		sqlSelect{
			public byte getByte(){return 31;}   
		},
		unknown{
			public byte getByte(){return 127;}   
		}; 
		
		
		public abstract byte getByte(); 
	};
	
	static public CMDEnumeration.command getCMD(byte b)
	{
		CMDEnumeration.command cmd = null;
 
		switch(b)
	    {
	    	case 0: 
	    		cmd = CMDEnumeration.command.start;
	    		break;
	    	case 1:
	    		cmd = CMDEnumeration.command.stop;
	    		break;
	    	case 2: 
	    		cmd = CMDEnumeration.command.createDB;
	    		break;
	    	case 3:
	    		cmd = CMDEnumeration.command.createTable;
	    		break;
	    	case 4:
	    		cmd = CMDEnumeration.command.openDB;
	    		break;
	    	case 5:
	    		cmd = CMDEnumeration.command.closeDB ;
	    		break;
	    	case 6:
	    		cmd = CMDEnumeration.command.insert;
	    		break;
	    	case 7:
	    		cmd = CMDEnumeration.command.delete;
	    		break;
	    	case 8:
	    		cmd = CMDEnumeration.command.ftQuery;
	    		break;
	    	case 9:
	    		cmd = CMDEnumeration.command.ptQuery;
	    		break;
	    	case 10:
	    		cmd = CMDEnumeration.command.rgQuery;
	    		break;
	    	case 18:
	    		cmd = CMDEnumeration.command.addFulltextColumn;
	    		break;
	    	case 19:
	    		cmd = CMDEnumeration.command.fetchRecordsDESC;
	    		break;
	    	case 20:
	    		cmd = CMDEnumeration.command.fetchRecordsASC;
	    		break;
	    	case 21:
	    		cmd = CMDEnumeration.command.fetchLog;
	    		break;
	    	case 22:
	    		cmd = CMDEnumeration.command.fetchTableNamesWithSuffix;
	    		break;	
	    	case 23:
	    		cmd = CMDEnumeration.command.notifySlavesUpdate;
	    		break;	
	    	case 24:
	    		cmd = CMDEnumeration.command.fetchQueryResultRecs;
	    		break;	
	    	case 25:
	    		cmd = CMDEnumeration.command.addAnalyticColumn;
	    		break;	
	    	case 26:
	    		cmd = CMDEnumeration.command.addStorableColumn;
	    		break;	
	    	case 27:
	    		cmd = CMDEnumeration.command.filterForWhereClause;
	    		break;	
	    	case 28:
	    		cmd = CMDEnumeration.command.getColumns;
	    		break;
	    	case 29:
	    		cmd = CMDEnumeration.command.recsCount;
	    		break; 
	    	case 30:
	    		cmd = CMDEnumeration.command.closeQueryResult;
	    		break; 
	    	case 31:
	    		cmd = CMDEnumeration.command.sqlSelect;
	    		break;
	    	case 127:
	    		cmd = CMDEnumeration.command.unknown;
	    		break;
	    	default:
	    		cmd = CMDEnumeration.command.unknown;
	    		break; 
	    }
		return cmd;
	}
	
	public static boolean needNotify(CMDEnumeration.command cmd)
	{
		switch(cmd)
		{
		case createDB:
		case createTable:
		case insert:
		case delete:
		case addFulltextColumn:
		case addAnalyticColumn:
		case addStorableColumn:
			return true; 
		}
		
		return false;
	}
}
