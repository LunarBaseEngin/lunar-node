package lunarion.cluster.coordinator.adaptor.converter;

import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.type.SqlTypeName;

public class MemoryData {
    
	public static Map<String, SqlTypeName> SQLTYPE_MAPPING = new HashMap<String, SqlTypeName>();
    public static Map<String, Class> JAVATYPE_MAPPING = new HashMap<String, Class>();

    static {
        initRowType(); 
    }
    
    public static void initRowType() {
        SQLTYPE_MAPPING.put("char", SqlTypeName.CHAR);
        JAVATYPE_MAPPING.put("char", Character.class);
        SQLTYPE_MAPPING.put("varchar", SqlTypeName.VARCHAR);
        JAVATYPE_MAPPING.put("varchar", String.class);
        SQLTYPE_MAPPING.put("boolean", SqlTypeName.BOOLEAN);
        SQLTYPE_MAPPING.put("integer", SqlTypeName.INTEGER);
        JAVATYPE_MAPPING.put("integer", Integer.class);
        SQLTYPE_MAPPING.put("tinyint", SqlTypeName.TINYINT);
        SQLTYPE_MAPPING.put("smallint", SqlTypeName.SMALLINT);
        SQLTYPE_MAPPING.put("bigint", SqlTypeName.BIGINT);
        SQLTYPE_MAPPING.put("decimal", SqlTypeName.DECIMAL);
        SQLTYPE_MAPPING.put("numeric", SqlTypeName.DECIMAL);
        SQLTYPE_MAPPING.put("float", SqlTypeName.FLOAT);
        SQLTYPE_MAPPING.put("real", SqlTypeName.REAL);
        SQLTYPE_MAPPING.put("double", SqlTypeName.DOUBLE);
        SQLTYPE_MAPPING.put("date", SqlTypeName.DATE);
        JAVATYPE_MAPPING.put("date", Date.class);
        SQLTYPE_MAPPING.put("time", SqlTypeName.TIME);
        SQLTYPE_MAPPING.put("timestamp", SqlTypeName.TIMESTAMP);
        SQLTYPE_MAPPING.put("any", SqlTypeName.ANY);
        /*
         * long is used in LunarBase as a 8 bytes integer
         */
        SQLTYPE_MAPPING.put("long", SqlTypeName.BIGINT);
        JAVATYPE_MAPPING.put("long", Long.class);
         
        JAVATYPE_MAPPING.put("string", String.class);
    }
    
     
    
    
}
