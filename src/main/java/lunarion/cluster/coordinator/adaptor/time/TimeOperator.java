package lunarion.cluster.coordinator.adaptor.time;

import java.sql.Date;
import java.util.Calendar;

 

public class TimeOperator {
	public int THE_YEAR(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return cal.get(Calendar.YEAR);
	}
	
/*	public int THE_YEAR(int date) {
		long mills = (long) date * (1000 * 3600 * 24);
		Date dt = Date.valueOf(DateFormat.formatToDateStr(mills));
		Calendar cal = Calendar.getInstance();
		cal.setTime(dt);
		return cal.get(Calendar.YEAR);
	}*/
	
	public Integer THE_MONTH(Date date) {
		return 6;
	}
	
	public Integer THE_DAY(Date date) {
		return 16;
	}
	
	public Integer THE_SYEAR(Date date, String type) {
		return 18;
	}
}