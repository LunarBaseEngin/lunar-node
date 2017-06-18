
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
package lunarion.cluster.coordinator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import LCG.RecordTable.StoreUtile.Record32KBytes;
import lunarion.db.local.shell.CMDEnumeration;
import lunarion.node.remote.protocol.MessageResponse;
import lunarion.node.remote.protocol.RemoteResult;
import lunarion.node.utile.ControllerConstants;

public class ResponseCollector {
	
	/*
	 * < table partition number, remote result>
	 * or
	 * < data piece id in remote results list, remote result>
	 */
	private ConcurrentHashMap<Integer, RemoteResult> response_map  ;
	private boolean succeed = true;
	
	private final boolean is_sql_result ;
	private ResultSet sql_select_result_set;
	
	private final long total_results;
	
	private final Resource db_resource;
	public ResponseCollector(Resource _db, ConcurrentHashMap<Integer, RemoteResult> _map)
	{
		is_sql_result = false;
		response_map = _map;
		
		Iterator<Integer> t = response_map.keySet().iterator();
		while(t.hasNext())
		{
			RemoteResult mr = response_map.get(t.next());
			if(mr == null || !mr.isSucceed())
				succeed = false;
		}
		
		long count = 0;
		Iterator<Integer> tt = response_map.keySet().iterator();
		while(tt.hasNext())
		{
			Integer t_result = tt.next();
			RemoteResult mr = response_map.get(t_result);
			count += (long)mr.getResultCount(); 
		} 
		
		total_results = count;
		db_resource = _db;
	}
	
	public void setFalse()
	{
		this.succeed = false;
	}
	public ResponseCollector(Resource _db, ResultSet _sql_result, long _total)
	{
		sql_select_result_set = _sql_result; 
		is_sql_result = true; 
		total_results = _total;
		db_resource = _db;
	} 
	
	public boolean isSqlResult()
	{
		return true;
	}
	
	public boolean isSucceed()
	{
		return succeed;
	}
	
	public RemoteResult getRemoteResult(Integer data_piece_i)
	{
		return this.response_map.get(data_piece_i);
	}
	
	public Iterator<Integer> getAllDataPieces()
	{
		return this.response_map.keySet().iterator();
	}
	
	public ArrayList<String[]> getUpdatedTables()
	{
		if(succeed && !is_sql_result)
		{
			ArrayList<String[]> updated_pieces = new ArrayList<String[]>();
			Iterator<Integer> tables = response_map.keySet().iterator();
			
			/*
			 * at least has one response.
			 */  
			while(tables.hasNext())
			{
				Integer table_i = tables.next();
				RemoteResult mr = response_map.get(table_i);
				if(mr!=null)
				{  
					System.out.println("updated table " +table_i+ " is: "+ mr.getParams()[1]);
					 
					String[]  db_and_table = new String[2];	 
					db_and_table[0] = mr.getDBName() ;
					db_and_table[1] = mr.getTableName() ;
					updated_pieces.add(db_and_table);
				}  
			}
			
			return updated_pieces;
		}
		else
			return null;
	}
	
	public long resultCount()
	{
		return total_results;
	}
	
	
	private ArrayList<String> fetchSqlRecords(int[] column_index, long from, int count) throws SQLException
	{
		ArrayList<String> records  = new ArrayList<String>(); 
		if(from > Integer.MAX_VALUE)
		{
			System.err.println("[COORDINATOR ERROR]: sql result set does not support positioning a cursor max than integer maximum value");
			return null;
		}
		if(column_index == null || column_index.length<=0)
		{
			ResultSetMetaData rsm = sql_select_result_set.getMetaData();  
			int col = rsm.getColumnCount();  
			column_index = new int[col];
			for (int i = 0; i < col; i++) 
			{  
				column_index[i] =  i + 1 ;  
			}
			/* 
			String col_name[] = new String[col]; 
			for (int i = 0; i < col; i++) 
			{  
				 col_name[i] = rsm.getColumnName( i + 1 );  
			}
			 */
		}
		 
		
		//if( sql_select_result_set.absolute((int)from))
		//{
			int i = -1;
			try {
				while(sql_select_result_set.next())
				{
						i++;
						if(i>=from && i < from+count)
						{
							String rec = ""; 
							rec = sql_select_result_set.getString(column_index[0]);
							 
							for(int kk=1;kk<column_index.length;kk++)
							{
								rec =  " | " + sql_select_result_set.getString(column_index[kk]); 
							} 	
							records.add(rec);
						}
						
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			return records;
			//} 
	}

	
	/*
	 * used in querying data that stored as what Resource.insert insert into partitions one by one.
	 * When one is full, moves to the next.
	 */
	public ArrayList<String> fetchRecordsForQueryResource(int[] _column_index, long from, int count)
	{
		if(is_sql_result)
		{  
			try {
				return fetchSqlRecords( _column_index, from, count);
			} catch (SQLException e) {
				 
				e.printStackTrace();
				return null;
			}
		}
		else
		{ 	 
			Enumeration<Integer> tables = response_map.keys();
			List<Integer> sorted_tables = new ArrayList<Integer>();
			while(tables.hasMoreElements())
			{
				sorted_tables.add(tables.nextElement());
			}
			
			Collections.sort(sorted_tables, new Comparator<Integer>(){
				  @Override
				  public int compare(Integer table_1, Integer table_2)
				  { 
					 		
					  if(table_1 < table_2 )
						  return -1;
					  if(table_1 == table_2 )
						  return 0;
					  else
						  return 1;
				  }
			  });
			
			ArrayList<String> records  = new ArrayList<String>(); 
			
			int partition_i = sorted_tables.size() -1 ;
			RemoteResult rr_i = response_map.get(sorted_tables.get(partition_i));
			int rec_count_in_partition_i = rr_i.getResultCount();
			/*
			 * same logic as @Resource.fetchRecords(String db, String table, long from, int count, boolean if_desc)
			 */
			/*
			 * recs returned from remote in each partition, having various counts, 
			 * and some partitions may have no result:
			 * |__1000_______|  |__2500_______|  |___100_______| ... |___750_____|
			 *   partition_0      partition_1      partition_5   ...  latest_partition
			 *                              from+count -------------------^ from position
			 */
			int begin_in_which_partition =  partition_i;
			int i_th_rec_count = rec_count_in_partition_i;
			long i_from = from;
			/*
			 * find out in which partition to begin fetching records
			 */
			while( i_from >= 0 )
			{
				//i_from = (i_from - i_th_rec_count )>0? ( i_from - i_th_rec_count ) : i_from;
				i_from = (i_from - i_th_rec_count );
				if(i_from >= 0)
				{
					/*
					 * move to the previous partition.
					 */
					begin_in_which_partition --;
					if(begin_in_which_partition < 0)
						return records;
					
					//i_th_rec_count =  max_datanumber.get();
					rr_i = response_map.get(sorted_tables.get(begin_in_which_partition));
					if(rr_i != null)
						i_th_rec_count  = rr_i.getResultCount();
					
				} 
			}
			
			i_from += i_th_rec_count;
			 
			int g_remains = count;
			while(g_remains > 0 && begin_in_which_partition >= 0)
			{
				int fetch_ith_iter = (i_th_rec_count - (int)i_from) >= g_remains? g_remains : (i_th_rec_count - (int)i_from);
				
				rr_i = response_map.get(sorted_tables.get(begin_in_which_partition));
				String[] recs_in_i = null;
				try {
					recs_in_i = rr_i.fetchQueryResult((int)i_from, fetch_ith_iter);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(recs_in_i != null)
				{
					for(String rec:recs_in_i)
					{
						records.add(rec);
					} 
				} 
		        	
				g_remains -= recs_in_i.length;
				if(begin_in_which_partition <= 0)
					return records;
				 
				/*
				 * seek previous partition for more records
				 */
				begin_in_which_partition --;
				if(begin_in_which_partition < 0)
					return records;
				i_from = 0;
				//i_th_rec_count = max_datanumber.get();
				while(begin_in_which_partition >= 0)
				{
					Integer table = sorted_tables.get(begin_in_which_partition);
					if(table != null)
					{
						rr_i = response_map.get(sorted_tables.get(begin_in_which_partition));
						if(rr_i != null)
						{
							i_th_rec_count  = rr_i.getResultCount();
							break;
						}
					} 
					begin_in_which_partition --;
					if(begin_in_which_partition < 0)
						return records;
				}
				
			}
			 
			return records; 
		} 
	} 
	
	/*
	 * for ResourceFullyDistributed, which inserts records according to data page with 1024 recs for each page. 
	 * When one page is full, moves to the next partition and its latest page.
	 */
	public ArrayList<String> fetchRecords (int[] _column_index, long from, int count)
	{
		if(is_sql_result)
		{  
			try {
				return fetchSqlRecords( _column_index, from, count);
			} catch (SQLException e) {
				 
				e.printStackTrace();
				return null;
			}
		}
		else
		{ 	 
			Enumeration<Integer> tables = response_map.keys();
			List<Integer> sorted_tables = new ArrayList<Integer>();
			while(tables.hasMoreElements())
			{
				sorted_tables.add(tables.nextElement());
			}
			
			Collections.sort(sorted_tables, new Comparator<Integer>(){
				  @Override
				  public int compare(Integer table_1, Integer table_2)
				  { 
					 		
					  if(table_1 < table_2 )
						  return -1;
					  if(table_1 == table_2 )
						  return 0;
					  else
						  return 1;
				  }
			  });
			
			ArrayList<String> records  = new ArrayList<String>(); 
			
			int partition_i = sorted_tables.size() -1 ;
			RemoteResult rr_i = response_map.get(sorted_tables.get(partition_i));
			int rec_count_in_partition_i = rr_i.getResultCount();
			/*
			 * same logic as @ResourceFullyDistributed.fetchRecords(String db, String table, long from, int count, boolean if_desc)
			 */ 
			/*
			 * recs in each partition for a query, dp(data page) has 1024 at most:
			 * level 0: |__|_dp 102 _|__|  |__|_dp  224_|__|  |__|_dp 998 _|__| ... |__|_dp 86 _|__|
			 * level 1: |__|_dp 646 _|__|  |__|_dp  324_|__|  |__|_dp 106 _|__| ... |__|_dp 624_|__|
			 * ...
			 * level n: |__|_dp 824 _|__|  |__|_dp  560_|__|
			 * 
			 *             partition_0         partition_1        partition_2      ...  partition_n
			 * 
			 *                             			^------the current partition in writing
			 * each data page has a bunch of records for a query.       
			 * 
			 * if fetch recs from position X, seek in which data piece, belonging to which partition, the position X is.  
			 *                   
			 */
			
			int begin_in_which_partition =  partition_i;
			int i_th_rec_count = rec_count_in_partition_i;
			long i_from = from;
			/*
			 * find out in which partition to begin fetching records
			 */
			while( i_from >= 0 )
			{
				//i_from = (i_from - i_th_rec_count )>0? ( i_from - i_th_rec_count ) : i_from;
				i_from = (i_from - i_th_rec_count );
				if(i_from >= 0)
				{
					/*
					 * move to the previous partition.
					 */
					begin_in_which_partition --;
					if(begin_in_which_partition < 0)
						return records;
					
					//i_th_rec_count =  max_datanumber.get();
					rr_i = response_map.get(sorted_tables.get(begin_in_which_partition));
					if(rr_i != null)
						i_th_rec_count  = rr_i.getResultCount();
					
				} 
			}
			
			i_from += i_th_rec_count;
			 
			int g_remains = count;
			while(g_remains > 0 && begin_in_which_partition >= 0)
			{
				int fetch_ith_iter = (i_th_rec_count - (int)i_from) >= g_remains? g_remains : (i_th_rec_count - (int)i_from);
				
				rr_i = response_map.get(sorted_tables.get(begin_in_which_partition));
				String[] recs_in_i = null;
				try {
					recs_in_i = rr_i.fetchQueryResult((int)i_from, fetch_ith_iter);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if(recs_in_i != null)
				{
					for(String rec:recs_in_i)
					{
						records.add(rec);
					} 
				} 
		        	
				g_remains -= recs_in_i.length;
				if(begin_in_which_partition <= 0)
					return records;
				 
				/*
				 * seek previous partition for more records
				 */
				begin_in_which_partition --;
				if(begin_in_which_partition < 0)
					return records;
				i_from = 0;
				//i_th_rec_count = max_datanumber.get();
				while(begin_in_which_partition >= 0)
				{
					Integer table = sorted_tables.get(begin_in_which_partition);
					if(table != null)
					{
						rr_i = response_map.get(sorted_tables.get(begin_in_which_partition));
						if(rr_i != null)
						{
							i_th_rec_count  = rr_i.getResultCount();
							break;
						}
					} 
					begin_in_which_partition --;
					if(begin_in_which_partition < 0)
						return records;
				}
				
			}
			 
			return records; 
		} 
	} 
	 
	public void printResponse()
	{
		if(is_sql_result)
		{
			try {
				System.out.println("total " + sql_select_result_set.getFetchSize() + " results." );
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				while (sql_select_result_set.next()) 
				{
					 System.out.println(sql_select_result_set.getString(1) + " | " 
							 			+ sql_select_result_set.getString(2) + " | " 
							 			+ sql_select_result_set.getString(3));
					 
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else
		{
			Iterator<Integer> tables = response_map.keySet().iterator();
			while(tables.hasNext())
			{
				Integer table = tables.next();
				RemoteResult mr = response_map.get(table);
				if(mr!=null)
				{
					System.out.println( table+ " responded: command "+ mr.getCMD());
					System.out.println( table+ " responded: UUID "+ mr.getUUID());
					System.out.println( table+ " responded: suceed "+ mr.isSucceed());
					for(int i=0;i<mr.getParams().length;i++)
					{
						 System.out.println( table+ " responded: "+ mr.getParams()[i]);
					} 
				}
				
			} 
		} 
	}
	
	public void closeQuery()
	{
		Iterator<Integer> pieces = response_map.keySet().iterator();
		while(pieces.hasNext())
		{
			Integer piece_i = pieces.next();
			RemoteResult mr = response_map.get(piece_i);
			if(mr!=null)
			{  
				//System.err.println("closing query for " + table);  
				this.db_resource.closeIntermediateQueryResult(mr);
				//System.err.println("closing query succeed? " + mm.isSucceed()); 
				 
			}
			
		} 
	}
}
