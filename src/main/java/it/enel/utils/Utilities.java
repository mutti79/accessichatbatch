package it.enel.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utilities {
	
	public static String getDateInString(Date data, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		String dataString = sdf.format(data);
		return dataString;
	}
	
	public static Date getStringInDate(String data, String format) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date dataDate = sdf.parse(data);
		return dataDate;
	}
	
	public static String getPartner(String partnerComplete) {
		String [] partnerDetail = partnerComplete.split("_");
		if(partnerDetail[partnerDetail.length-1].contains("wde") || partnerDetail[partnerDetail.length-1].contains("WDE")){
			String partnerOut = "";
			for(int j = 0; j < partnerDetail.length-1; j++) {
				partnerOut += partnerDetail[j]+"_";
			}
			return partnerOut.substring(0, partnerOut.length()-1);
		} else {
			return partnerComplete;
		}
	}
	
	public static String getServizio(String partnerComplete) {
		String [] partnerDetail = partnerComplete.split("_");
		if(partnerDetail[partnerDetail.length-1].contains("wde") || partnerDetail[partnerDetail.length-1].contains("WDE")){
			return partnerDetail[partnerDetail.length-1];
		} else {
			return "";
		}
	}
}
