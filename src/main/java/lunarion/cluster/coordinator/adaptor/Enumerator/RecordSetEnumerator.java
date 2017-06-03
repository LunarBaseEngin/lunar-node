package lunarion.cluster.coordinator.adaptor.Enumerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import com.google.common.base.Throwables;

import LCG.DB.API.LunarDB;
import LCG.MemoryIndex.IndexTypes.DataTypes;
import LCG.RecordTable.StoreUtile.LunarColumn;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import lunarion.cluster.coordinator.Resource;
import lunarion.cluster.coordinator.ResponseCollector;
import lunarion.cluster.coordinator.adaptor.converter.ArrayRecordConverter;
import lunarion.cluster.coordinator.adaptor.converter.RecordConverter;  

public class RecordSetEnumerator<E> implements Enumerator<E> { 
	
	private E current;
	/*
	 * begin with -1, otherwise, lost the first record
	 */
	private AtomicLong current_rec_id = new AtomicLong(-1);
	
	private final Resource db_inst;
	private final String table_name;
	private RecordConverter<E> rec_converter;
	//int[] fields; 
	private String[] columns;
	
	private HashMap<String, String> col_type_map ;
	
	final private long total_results;
	
  	  
	/*
	 * for caching recored, to avoid fetching from remote frequently 
	 */
	final long maximum_cached = 1024;
	final long maximum_cached_mask = (~(maximum_cached-1));
	long cached_from = 0; 
	ArrayList<String> cached_recs = null;
	int current_cached = 0;
	
	public RecordSetEnumerator(Resource _db_res,
								String _table, 
								String[] _columns,
								HashMap<String, String> _col_type_map ) 
	{ 
		 
		this.db_inst = _db_res;
		this.table_name = _table;
		 
		//fields = _fields;
		columns = _columns;
		col_type_map = _col_type_map;
		total_results = db_inst.recsCount(table_name);
		rec_converter = (RecordConverter<E>) new ArrayRecordConverter(columns, col_type_map);
	}
	
	 
	
	public E current() {
		ResponseCollector result;  
		  
		if(current_rec_id.get() >= cached_from 
				&& current_rec_id.get() < (cached_from + current_cached) 
				&& cached_recs != null)
		{
			String rec = cached_recs.get((int)(current_rec_id.get() - cached_from));
			if(rec != null && !rec.equals(""))
				current = rec_converter.convertRecord(rec);
		}
		else
		{
			cached_from = ((current_rec_id.get())&maximum_cached_mask);
			result = db_inst.fetchRecords( db_inst.getDBName() , table_name, cached_from, (int)maximum_cached, true);
			
			
			//cached_recs = query_result.fetchRecords(null, cached_from, (int)maximum_cached);
			/*
			 * result has maximum_cached records at most. 
			 */
			cached_recs = result.fetchRecords(null, 0, (int)maximum_cached);
			
			if (cached_recs ==null || cached_recs.isEmpty()) 
			{  
				current = null;  
			}
			else
			{
				current_cached = cached_recs.size();
				
				String rec = cached_recs.get((int)(current_rec_id.get() - cached_from));
				if(rec!=null && !rec.equals(""))
					current = rec_converter.convertRecord(rec);
				else
					current = null;
			} 
		}  
	    return current;
	  
	}
	public boolean moveNext() 
	{ 
		//return current_rec_id.incrementAndGet() < db_inst.recsCount(table_name);
		return current_rec_id.incrementAndGet() < total_results;
	}

	
	public void reset() {
	    throw new UnsupportedOperationException();
	}
	
	public void close() { 
	     
	}
	
	 
	 
}
