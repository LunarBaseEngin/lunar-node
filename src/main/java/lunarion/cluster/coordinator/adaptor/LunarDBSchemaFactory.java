package lunarion.cluster.coordinator.adaptor;

import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.ConversionUtil;

 
public class LunarDBSchemaFactory implements SchemaFactory {
	 
	 
	public Schema create(SchemaPlus parentSchema, String name,
			Map<String, Object> operand) 
	{
		System.out.println("flavor : " + operand.get("flavor"));
		System.out.println("full_path : " + operand.get("full_path")); 
		System.out.println("column0 : " + operand.get("column0"));
		System.out.println("column1 : " + operand.get("column1"));
		
		String full_path = (String)  operand.get("full_path") ;
				
		System.out.println("Get database " + name + " at "+ full_path );
		
		String flavor_name = (String) operand.get("flavor");
		LunarScannableTable.Flavor flavor;
	    if (flavor_name == null 
	    		|| flavor_name.equalsIgnoreCase(LunarScannableTable.Flavor.SCANNABLE.name())) {
	    	flavor = LunarScannableTable.Flavor.SCANNABLE;
	    } 
	    else if (flavor_name.equalsIgnoreCase(LunarScannableTable.Flavor.FILTERABLE.name())){
	    	flavor = LunarScannableTable.Flavor.FILTERABLE;
	    }
	    else
	    {
	    	flavor = LunarScannableTable.Flavor.SCANNABLE;
	    }
		
	    /*
	     * set for default utf-8 encoding and decoding, 
	     * thus supports languages along with English.
	     */
	    System.setProperty("saffron.default.charset",ConversionUtil.NATIVE_UTF16_CHARSET_NAME); 
		System.setProperty("saffron.default.nationalcharset",ConversionUtil.NATIVE_UTF16_CHARSET_NAME); 
		System.setProperty("saffron.default.collation.name",ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");
		
	    return new LunarDBSchema(name, flavor );
	}
}
