package it.enel.dbconnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class Connessione {
	
	static Logger log = Logger.getLogger(Connessione.class);
	
	private String user;
	private String password;
	private String driver;
	private String url;
	private Connection con;
	
	public Connessione(String driver, String url, String user, String password) {
		con = null;
		this.driver = driver; 
		this.url = url; 
		this.user = user;
		this.password = password;
	}

	public Connection creaConnessione() throws Exception {
		try {
			log.debug("Caricamento driver: " + driver + "...");
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			log.error("Errore durante il caricamento del driver Jdbc: ", e);
			throw e;
		}

		try {
			log.debug("Caricamento url: " + url + "...");
			con = DriverManager.getConnection(url, user, password);

		} catch (SQLException e) {
			log.error("Errore durante la creazione della connessione: ", e);
			con = null;
		}

		return con;
	}
	
	public void chiudiConnessione() {
		try {
			con.close();
		} catch (SQLException e) {
			log.error("Errore durante la chiusura della connessione: ", e);
		} catch (NullPointerException e) {
			log.error("Chiamato getConnection senza preventiva createConnection: ", e);
		}
	}
	
	public Connection getConnection() {
		return con;
	}
}
