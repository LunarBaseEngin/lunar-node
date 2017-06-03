package lunarion.cluster.coordinator.adaptor.converter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import LCG.RecordTable.StoreUtile.Record32KBytes; 

public abstract class RecordConverter<E>  {
	 
	private static final SimpleDateFormat TIME_FORMAT_DATE = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat TIME_FORMAT_TIME = new SimpleDateFormat("HH:mm:ss"); 
	private static final SimpleDateFormat TIME_FORMAT_TIMESTAMP = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	 
	
	public abstract E convertRecord( String rec);

	}

		  

		  

		 
