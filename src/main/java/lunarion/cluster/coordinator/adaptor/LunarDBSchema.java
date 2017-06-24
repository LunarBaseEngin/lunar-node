package lunarion.cluster.coordinator.adaptor;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import LCG.DB.API.LunarDB;
import lunarion.cluster.coordinator.Coordinator;
import lunarion.cluster.coordinator.server.CoordinatorServer;
import lunarion.cluster.resource.Resource;
import lunarion.cluster.resource.ResourceDistributed; 

public class LunarDBSchema extends AbstractSchema {
	  
	private final LunarScannableTable.Flavor flavor;
	private final String db_name;
	
	public LunarDBSchema(String _db_name, LunarScannableTable.Flavor _flavor ) { 
	   
	     
	    flavor = _flavor;
	    db_name = _db_name;
	    
	}
	
	@Override 
	protected Map<String, Table> getTableMap() {
		final Map<String, Table> builder = new HashMap<String, Table>();
		
		ResourceDistributed res = Coordinator.getInstance().getResource(db_name);
	    
	    
		
		Iterator<String> tables = res.listTables();
		if(tables == null)
		{
			System.err.println("[ERROR]: @LunarDBSchema.getTableMap can not get tabls");
			return null;
		}
		
		while(tables.hasNext())
		{
			String table_name = tables.next();
			switch (flavor) {
		    case TRANSLATABLE:
		    	{
		    		//LunarScannableTable lst = new LunarScannableTable(res, table_name );
		    		//builder.put(table_name, lst );
		    	}
		    	break;
		    case SCANNABLE:
		    	{
		    		//LunarScannableTable lst = new LunarScannableTable(res, table_name );
		    		//builder.put(table_name, lst);
		    	}
		    	break;
		    case FILTERABLE:
		    	{
		    		LunarFilterableTableRemote lft = new LunarFilterableTableRemote(res, table_name );
		    		builder.put(table_name, lft);
		    	}break;
		    default:
		      throw new AssertionError("Unknown flavor " + flavor);
		    }
		  
		}
		return builder ; 
	}  
}
