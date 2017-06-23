package lunarion.cluster.coordinator.adaptor;

 
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List; 

import org.apache.calcite.DataContext; 
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator; 
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;  
import org.apache.calcite.sql.SqlKind; 

import LCG.DB.API.LunarDB;
import LCG.DB.API.Result.FTQueryResult;
import LCG.DB.API.Result.RGQueryResult;
import LCG.MemoryIndex.IndexTypes.DataTypes;
import lunarion.cluster.coordinator.Resource;
import lunarion.cluster.coordinator.ResourceDistributed;
import lunarion.cluster.coordinator.ResponseCollector;
import lunarion.cluster.coordinator.adaptor.Enumerator.QueryResultEnumerator;
import lunarion.cluster.coordinator.adaptor.Enumerator.RecordSetEnumerator;
import lunarion.cluster.coordinator.adaptor.converter.TripleOperator; 

public class LunarFilterableTableRemote extends LunarAbstractTable implements FilterableTable  {
	
	
	public LunarFilterableTableRemote(ResourceDistributed _db_res, String _table_name ) {
		super(_db_res, _table_name);  
	} 
	 
	public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
		//final String[] filterValues = new String[this.dataType.getFieldCount()];
		LogicUtileRemote lu = new LogicUtileRemote(this.column_names_and_types, db_resource, table_name);
		
		for (final Iterator<RexNode> i = filters.iterator(); i.hasNext();) 
		{
		      final RexNode filter = i.next();
		       
		      final TripleOperator t_o  = lu.isSimpleBinary(filter);
		      if (t_o != null ) {
		    	  i.remove(); 
		    	  
		    	  final ResponseCollector result = lu.queryRemote(t_o); 
		    	  
		    	  return new AbstractEnumerable<Object[]>() {
		              public Enumerator<Object[]> enumerator() {
		                  return new QueryResultEnumerator<Object[]>(result, column_names, col_type_map );
		              }
		          }; 
		      }
		      else
		      {
		    	  final String logic_statement = lu.isSimpleLogic(filter) ;
		    	  if(logic_statement != null)
		    	  {
		    		  i.remove(); 
		    		  ResponseCollector logic_result = lu.queryRemote(logic_statement) ;
		    		 
		    		  return new AbstractEnumerable<Object[]>() {
			              public Enumerator<Object[]> enumerator() {
			                  return new QueryResultEnumerator<Object[]>(logic_result, column_names, col_type_map );
			              }
			          }; 
		    	  } 
		      }
		} 
		
	 
		 
       /*
        * if incomes an empty filter, happens when join two tables, e.g. where table1.id=table2.id, 
        * then for each table, the filters are empty, then scan the whole table.
        */
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new RecordSetEnumerator<Object[]>(db_resource, table_name, column_names, col_type_map );
            }
        }; 
	}  
	
	 
	
}
