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
import lunarion.cluster.coordinator.Resource;
import lunarion.cluster.coordinator.ResourceDistributed;
import lunarion.cluster.coordinator.adaptor.converter.MemoryData; 

public class LunarAbstractTable extends AbstractTable {
	
	protected String table_name;
	//protected Resource db_resource;
	protected ResourceDistributed db_resource;
	protected String[] column_names_and_types;
	protected String[] column_names ;
	
	/*
	 * <column name, type >
	 */
	protected HashMap<String, String> col_type_map = new HashMap<String,String>();
	
	protected RelDataType dataType; 

	public LunarAbstractTable(ResourceDistributed _db_resource, String _table_name ) {
	    this.table_name = _table_name;
	    this.db_resource = _db_resource;  
	    this.dataType = null; 
	}
	 
	protected static int[] identityList(int n) { 
		int[] integers = new int[n];
	    for (int i = 0; i < n; i++) {
	           integers[i] = i;
	    }	    
	    return integers;
	} 
	 
	public RelDataType getRowType(RelDataTypeFactory typeFactory) { 
		
		if(dataType == null) 
		{
            RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
            column_names_and_types = db_resource.getTableColumns(table_name);
            column_names = new String[column_names_and_types.length/2];
              
            for(int i=0; i<column_names_and_types.length; i+=2)
			{  
				//DataTypes col_type = db_resource.getColumnDataType(table_name, column_names[i]);
			 	
				col_type_map.put(column_names_and_types[i],column_names_and_types[i+1]);
				column_names[i/2] = column_names_and_types[i];
				String lunar_column_type_in_string = column_names_and_types[i+1].toLowerCase();
				RelDataType sqlType = typeFactory.createJavaType(
                        MemoryData.JAVATYPE_MAPPING.get(lunar_column_type_in_string));
				
				sqlType = SqlTypeUtil.addCharsetAndCollation(sqlType, typeFactory);
                fieldInfo.add(column_names_and_types[i], sqlType);
			}
             
            this.dataType = typeFactory.createStructType(fieldInfo);
        }
        return this.dataType; 
		 
	} 
	
	public enum Flavor {
		    SCANNABLE, FILTERABLE, TRANSLATABLE
	}
}
