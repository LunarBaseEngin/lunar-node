package lunarion.cluster.coordinator.adaptor.Enumerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import com.google.common.base.Throwables;

import LCG.DB.API.LunarDB;
import LCG.DB.API.Result.FTQueryResult; 
import LCG.MemoryIndex.IndexTypes.DataTypes;
import LCG.RecordTable.StoreUtile.LunarColumn;
import LCG.RecordTable.StoreUtile.Record32KBytes;
import lunarion.cluster.coordinator.adaptor.converter.ArrayRecordConverter;
import lunarion.cluster.coordinator.adaptor.converter.RecordConverter;
import lunarion.cluster.resource.ResponseCollector;  

public class QueryResultEnumerator<E> implements Enumerator<E> { 
	
	private E current;
	
	/*
	 * begin with -1, otherwise, lost the first record
	 */
	private AtomicLong current_rec_id = new AtomicLong(-1);
	
	//private final LunarDB db_inst;
	//private final String table_name;
	ResponseCollector query_result;
	
	private RecordConverter<E> rec_converter;
	 
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
	  
	public QueryResultEnumerator(ResponseCollector _query_result,
								String[] _columns,
								HashMap<String, String> _col_type_map ) 
	{ 
		 
		//this.db_inst = _db;
		//this.table_name = _table;
		//db_inst.openTable(table_name); 
		
		query_result = _query_result;
		total_results = _query_result.resultCount();
		//fields = _fields;
		//columnTypes = _types;
		columns = _columns;
		col_type_map = _col_type_map;
		
		rec_converter = (RecordConverter<E>) new ArrayRecordConverter(columns, col_type_map);
	}
	
	 
	
	public E current() {
		if(query_result == null || query_result.resultCount() <=0)
			return null;
		
		
	 
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
			cached_recs = query_result.getRecordsForCMDQuery(null, cached_from, (int)maximum_cached);
			
			if (cached_recs ==null || cached_recs.isEmpty()) 
			{  
				current = null;  
			}
			else
			{
				current_cached = cached_recs.size();
				if(current_cached == 0 || (current_rec_id.get() - cached_from ) >= current_cached)
				{
					current = null;
					 
					return current;
				}
				
				
				String rec = cached_recs.get((int)(current_rec_id.get() - cached_from));
				if(rec!=null && !rec.equals(""))
					current = rec_converter.convertRecord(rec);
				else
					current = null;
			} 
		} 
 
		//System.err.println(current_rec_id.get());
	    return current;
	  
	}
	public boolean moveNext() 
	{ 
		return current_rec_id.incrementAndGet() < total_results;
	}

	
	public void reset() {
	    throw new UnsupportedOperationException();
	}
	
	public void close() {  
		
		query_result.closeQuery();
	}
	
	 
	 
}
