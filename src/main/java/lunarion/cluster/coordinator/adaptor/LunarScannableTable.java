package lunarion.cluster.coordinator.adaptor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeUtil;

import LCG.DB.API.LunarDB;
import LCG.MemoryIndex.IndexTypes.DataTypes;
import lunarion.cluster.coordinator.adaptor.Enumerator.RecordSetEnumerator;
import lunarion.cluster.resource.Resource;
import lunarion.cluster.resource.ResourceDistributed; 

public class LunarScannableTable extends LunarAbstractTable implements ScannableTable {
	
	//private String table_name;
	//private LunarDB db_instance;
	
	
	//private RelDataType dataType;
    
	//protected List<CsvFieldType> fieldTypes;
	/*
	 * <column name, type>
	 */
	//protected HashMap<String, CsvFieldType> fieldTypes;

	public LunarScannableTable(ResourceDistributed _db_res, String _table_name ) {
	    super(_db_res, _table_name);
	    
	} 

	public Enumerable<Object[]> scan(DataContext root) {
		String[] columns = db_resource.getTableColumns(table_name);
		final HashMap<String, String> col_type_map = new HashMap<String,String>();
		
		 
		for(int i=0; i<columns.length; i+=2)
		{  
			col_type_map.put(columns[i],columns[i+1].toLowerCase()); 
		}
		 
        final int[] fields = identityList(this.dataType.getFieldCount());
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new RecordSetEnumerator<Object[]>(db_resource, table_name, column_names, col_type_map );
            }
        }; 
	} 
	
	 
}
