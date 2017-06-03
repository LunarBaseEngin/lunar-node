package lunarion.cluster.coordinator.adaptor.converter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import LCG.RecordTable.StoreUtile.Record32KBytes;
 
import lunarion.cluster.coordinator.adaptor.Enumerator.MemoryEnumerator;

public class ArrayRecordConverter extends RecordConverter<Object[]> {
	//private final CsvFieldType[] fieldTypes;
	//private final int[] fields;
	private final HashMap<String, String> col_type_map; 
	private final String[] columns;
	public ArrayRecordConverter(String[] _columns, HashMap<String, String> _col_type_map) 
	{
		//this.fieldTypes = fieldTypes.toArray(new CsvFieldType[fieldTypes.size()]);
		//this.fields = fields;
		
		col_type_map = _col_type_map;
		columns =  _columns;
	} 
			   
	@Override
	public Object[] convertRecord(String rec_in_string) {
        
		Object[] objects = new Object[columns.length]; 
		Record32KBytes rec = new Record32KBytes(-1, rec_in_string);
		
		/*
		 * MUST generate object according to the sequence of how columns array ordered 
		 */
		for(int i =0;i<columns.length;i++)
		{
			String col = columns[i];
			String val = rec.getColumn(col).getColumnValue();
			objects[i] = MemoryEnumerator.convertOptiqCellValue(val, col_type_map.get(col));
		}
		
		/*
		Object[] objects = new Object[col_type_map.keySet().size()]; 
		
		Record32KBytes rec = new Record32KBytes(-1, rec_in_string);
		
        Iterator<String> col_iter = rec.getColumns().keySet().iterator();
        int i = 0 ; 
        while(col_iter.hasNext())
        {
        	String col = col_iter.next();
        	String val =  rec.getColumns().get(col).getColumnValue();
        	objects[i] = MemoryEnumerator.convertOptiqCellValue(val, col_type_map.get(col));
        	i ++;
            
        } 
        */
        return objects;
    }

			    
	 

	 
}

 
