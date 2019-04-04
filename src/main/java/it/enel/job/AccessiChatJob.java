package it.enel.job;

import java.io.FileInputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import it.enel.batch.AccessiChatMain;
import it.enel.batch.Constants;
import it.enel.dbconnection.AccessiChatDao;
import it.enel.dbconnection.Connessione;
import it.enel.model.AccessiChatDettagli;
import it.enel.utils.Utilities;

public class AccessiChatJob {

	static Logger log = Logger.getLogger(AccessiChatMain.class);
	static Properties properties=null;
	
	public AccessiChatJob() throws Exception{
        
		String fileName = Constants.DEFAULT_FILENAME;
		properties = new Properties();
		try {
			properties.load(new FileInputStream(fileName));
			PropertyConfigurator.configure(properties.getProperty(Constants.LOG4J_FILE).trim());
			
		} catch (Exception e) {
			log.error("error in creating batch process...", e);
			throw e;
		}
	}

	public static void execute(String[] args){

		long startms=System.currentTimeMillis();
		try{
			new AccessiChatJob();
			log.info("executeAccessiChat - START batch AccessiChat-UP");
			
			String driverRead = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_DRIVER_READ);
			String urlRead = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_URL_READ);
			String userRead = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_USER_READ);
			String passwordRead = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_PASSWORD_READ);
			Connessione conRead = new Connessione(driverRead, urlRead, userRead, passwordRead);
			
			String driverWrite = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_DRIVER_WRITE);
			String urlWrite = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_URL_WRITE);
			String userWrite = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_USER_WRITE);
			String passwordWrite = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_PASSWORD_WRITE);
			Connessione conWrite = new Connessione(driverWrite, urlWrite, userWrite, passwordWrite);

			AccessiChatDao dao = new AccessiChatDao();
			
			String format = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_FORMATO_DATA);
			//1. recuperare max data
			Date maxData = dao.getMaxDataFromAccessiChatDetail(conWrite);
			
			//Calcolo di "oggi" senza ore
			Date oggi = Utilities.getStringInDate(Utilities.getDateInString(new Date(), format), format);
			
			//Calendar per calcolo di "ieri"
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE, -1);
			Date ieri = Utilities.getStringInDate(Utilities.getDateInString(cal.getTime(), format), format);
			
			//2.1 max data vuota? primo lancio batch
			if(maxData == null) {
				//2.1.1 prendo periodo e suddivido per giorni
				String dataInizioString = AccessiChatJob.properties.getProperty(Constants.ACCESSI_CHAT_DATA_INIZIO);
				if(dataInizioString.isEmpty()) {
					log.error("DATA_INIZIO e' vuota! Il batch verra' terminato!");
					return;
				}
				Date dataInizio = Utilities.getStringInDate(dataInizioString, format);
				while(dataInizio.before(oggi)) {
					//2.1.1.1 calcolo del giorno successivo alla dataFine
					cal.setTime(dataInizio);
					cal.add(Calendar.DATE, 1);
					Date dataFine = cal.getTime();
					//2.1.2 chiamo query per recuparare
					List<AccessiChatDettagli> lista = dao.getAccessiChatNewData(conRead, dataInizio, dataFine, format);
					//2.1.3 faccio gli insert per il giorno corrente
					if(lista == null || lista.isEmpty()) {
						dao.insertIndividuallyDataIntoAccessiChatDetail(conWrite, Utilities.getDateInString(dataInizio, format), Utilities.getDateInString(dataInizio, format));
					} else {
						int inseriti = dao.insertIndividuallyDataIntoAccessiChatDetail(conWrite, lista);
						log.info("Sono stati inseriti " + inseriti + " correttamente per il giorno: " + dataInizio);
					}
					//2.1.3.1 aggiornamento dataInizio a giorno successivo
					dataInizio = dataFine;
				}
			} 
			//2.2 max data popolata e minore di ieri? lancio batch giornaliero
			else if (maxData != null && maxData.before(ieri)) {
				//2.2.2 chiamo query per recuparare
				List<AccessiChatDettagli> lista = dao.getAccessiChatNewData(conRead, ieri, oggi, format);
				//2.2.3 faccio gli insert per il giorno corrente
				if(lista == null || lista.isEmpty()) {
					dao.insertIndividuallyDataIntoAccessiChatDetail(conWrite, Utilities.getDateInString(ieri, format), Utilities.getDateInString(oggi, format));
				} else {
					int inseriti = dao.insertIndividuallyDataIntoAccessiChatDetail(conWrite, lista);
					log.info("Sono stati inseriti " + inseriti + " correttamente per il giorno: " + ieri);
				}
				
				//Se si vuole continuare un thread interrotto (da testare!):
				/*while(maxData.before(oggi)) {
					//2.1.1.1 calcolo del giorno successivo alla dataFine
					cal.setTime(maxData);
					cal.add(Calendar.DATE, 1);
					Date dataFine = cal.getTime();
					//2.1.2 chiamo query per recuparare
					List<AccessiChatDettagli> lista = dao.getAccessiChatNewData(conRead, maxData, dataFine, format);
					//2.1.3 faccio gli insert per il giorno corrente
					if(lista == null || lista.isEmpty()) {
						dao.insertIndividuallyDataIntoAccessiChatDetail(conWrite, Utilities.getDateInString(maxData, format), Utilities.getDateInString(maxData, format));
					} else {
						int inseriti = dao.insertIndividuallyDataIntoAccessiChatDetail(conWrite, lista);
						log.info("Sono stati inseriti " + inseriti + " correttamente per il giorno: " + maxData);
					}
					//2.1.3.1 aggiornamento dataInizio a giorno successivo
					maxData = dataFine;
				}*/
				
			}
			//2.3 max data popolata e uguale a ieri? non faccio nulla
			else if (maxData != null && maxData.equals(ieri)) {
				log.info("Il batch ha gia' girato nelle ultime 24h.");
			}

		} catch (Exception e) {
			log.error("executeAccessiChat - ERROR", e);
		} catch(Throwable t) {
			log.error("executeAccessiChat - ERROR - Throwable", t);
		} finally{
			long endms=System.currentTimeMillis();
			log.info("executeAccessiChat - END batch AccessiChat-UP in " + (endms-startms));
		}
	}

}
