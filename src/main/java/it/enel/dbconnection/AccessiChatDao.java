package it.enel.dbconnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import it.enel.model.AccessiChatDettagli;
import it.enel.utils.Utilities;

public class AccessiChatDao {

	static Logger log = Logger.getLogger(AccessiChatDao.class);

	PreparedStatement ps = null;
	Statement s = null;
	ResultSet rs = null;

	public Date getMaxDataFromAccessiChatDetail(Connessione con) {//select to_char(max(to_date(substr(periodo, 1, 10),'dd/mm/yyyy')),'dd/mm/yyyy') from ACCESSI_CHAT_DETTAGLI
		Date data = null;
		try {
			ps=con.creaConnessione().prepareStatement("select max(to_date(substr(periodo, 14, 10),'dd/mm/yyyy')) from ACCESSI_CHAT_DETTAGLI");
			rs = ps.executeQuery();
			if (rs.next()) {
				data = rs.getDate(1);
			}
		} catch (SQLException e) {
			log.error("Errore sql: ", e);
		} catch (Exception e) {
			log.error("Errore non gestito: ", e);
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				log.error("Errore durante la chiusura dello statement: ", e);
			}
			con.chiudiConnessione();			
		}
		return data;
	}

	public int insertDataIntoAccessiChatDetail(Connessione con, AccessiChatDettagli dettaglio) {
		int numeroRighe = 0;
		try {
			ps=con.creaConnessione().prepareStatement("insert into ACCESSI_CHAT_DETTAGLI (PERIODO, PARTNER, SERVIZIO, QP, CORTESIA, ESIGENZA, CHIAREZZA, RIS_PROBLEMA, TEMPO_ATTESA, VAL_ESPERIENZA)"
					+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			ps.setString(1, dettaglio.getPeriodo());
			ps.setString(2, dettaglio.getPartner());
			ps.setString(3, dettaglio.getServizio());
			ps.setString(4, dettaglio.getQp());
			ps.setString(5, dettaglio.getCortesia());
			ps.setString(6, dettaglio.getEsigenza());
			ps.setString(7, dettaglio.getChiarezza());
			ps.setString(8, dettaglio.getRisProblema());
			ps.setString(9, dettaglio.getTempoAttesa());
			ps.setString(10, dettaglio.getValEsperienza());

			numeroRighe = ps.executeUpdate();
		} catch (SQLException e) {
			log.error("Errore sql: ", e);
		} catch (Exception e) {
			log.error("Errore non gestito: ", e);
		} finally {
			try {
				if(ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				log.error("Errore durante la chiusura dello statement: ", e);
			}
			con.chiudiConnessione();			
		}

		return numeroRighe;
	}

	public int insertIndividuallyDataIntoAccessiChatDetail(Connessione con, List<AccessiChatDettagli> dettagli) {
		int inseriti = 0;

		for(AccessiChatDettagli dettaglio : dettagli) {
			inseriti += insertDataIntoAccessiChatDetail(con, dettaglio);
		}

		return inseriti;
	}
	
	public int insertIndividuallyDataIntoAccessiChatDetail(Connessione con, String dataInizio, String dataFine) {
		int inseriti = 0;

		AccessiChatDettagli dettaglio = new AccessiChatDettagli();
		dettaglio.setPeriodo(dataInizio + " - " + dataInizio);
		inseriti += insertDataIntoAccessiChatDetail(con, dettaglio);

		return inseriti;
	}
	
	public boolean insertDataIntoAccessiChatDetail(Connessione con, List<AccessiChatDettagli> dettagli) {
		boolean inseriti = false;

		String sqlBegin = "insert into ACCESSI_CHAT_DETTAGLI (PERIODO, PARTNER, SERVIZIO, QP, CORTESIA, ESIGENZA, CHIAREZZA, RIS_PROBLEMA, TEMPO_ATTESA, VAL_ESPERIENZA)"
				+ " values (";
		String sqlEnd = "); ";

		String sqlTotal="BEGIN ";

		for(int i = 0; i < dettagli.size(); i++) {
			String dettaglio = "'" + dettagli.get(i).getPeriodo() + "', '" +
					"'" + dettagli.get(i).getPartner() + "', '" +
					"'" + dettagli.get(i).getServizio() + "', '" +
					"'" + dettagli.get(i).getQp() + "', '" +
					"'" + dettagli.get(i).getCortesia() + "', '" +
					"'" + dettagli.get(i).getEsigenza() + "', '" +
					"'" + dettagli.get(i).getChiarezza() + "', '" +
					"'" + dettagli.get(i).getRisProblema() + "', '" +
					"'" + dettagli.get(i).getTempoAttesa() + "', '" +
					"'" + dettagli.get(i).getValEsperienza() + "', '";

			sqlTotal += sqlBegin + dettaglio + sqlEnd;
		}
		sqlTotal += "END;";

		try {
			s=con.creaConnessione().createStatement();
			inseriti = s.execute(sqlTotal);

		} catch (SQLException e) {
			log.error("Errore sql: ", e);
		} catch (Exception e) {
			log.error("Errore non gestito: ", e);
		} finally {
			try {
				if(s != null) {
					s.close();
				}
			} catch (SQLException e) {
				log.error("Errore durante la chiusura dello statement: ", e);
			}
			con.chiudiConnessione();			
		}

		return inseriti;
	}

	public List<AccessiChatDettagli> getAccessiChatNewData(Connessione con, Date dataInizio, Date dataFine, String dateFormat){
		List<AccessiChatDettagli> dettagli = new ArrayList<AccessiChatDettagli>();

		// query 1 - media voto delle singole domande per partner

		try {
		String sql = "select distinct "
				+ " (case when acn.virtual_Chat = 1 then (acn.id_Partner || ' + VA') else acn.id_Partner end),"
				+ " (case when acn.id_Partner <> 'VIRTUAL_ASSISTANT' and (acn.virtual_Chat is null or acn.virtual_Chat = 0) and acvd.id_Domanda = 6 then null else acvd.id_Domanda end), "
				+ " round(sum(acv.voto * acvd.peso) * 1.0D/sum(acvd.peso),2) "
				+ " from Accessi_Chat_New acn "
				+ " join accessi_Chat_Valut_New acv on acn.id_chat=acv.id_chat"
				+ " join accessi_Chat_Valut_Descr acvd on acv.cod_domanda=acvd.id_domanda"
				+ " where acn.data_Richiesta_Chat is not null "
				+ " and acn.id_Partner is not null "
				+ " and acvd.flg_Attiva = 1"
				+ " and acn.data_Richiesta_Chat >= to_date(?, 'dd/MM/yyyy')"
				+ " and acn.data_Richiesta_Chat <= to_date(?, 'dd/MM/yyyy')"
				+ " group by (case when acn.virtual_Chat = 1 then (acn.id_Partner || ' + VA') else acn.id_Partner end), "
				+ " (case when acn.id_Partner <> 'VIRTUAL_ASSISTANT' and (acn.virtual_Chat is null or acn.virtual_Chat = 0) and acvd.id_Domanda = 6 then null else acvd.id_Domanda end)";

		ps = con.creaConnessione().prepareStatement(sql);
		ps.setString(1, Utilities.getDateInString(dataInizio, dateFormat));
		ps.setString(2, Utilities.getDateInString(dataFine, dateFormat));

		rs = ps.executeQuery();
		
		List<AccessiChatDettagli> dettagliApp = new ArrayList<AccessiChatDettagli>();
		while(rs.next()) {
			AccessiChatDettagli dettaglio = new AccessiChatDettagli();
			dettaglio.setPartner(rs.getString(1));
			dettaglio.setCodDomanda(rs.getLong(2));
			dettaglio.setQp(rs.getString(3));
			
			dettagliApp.add(dettaglio);
		}
		
		if(rs != null) {
			rs.close();
		}
		if(ps != null) {
			ps.close();
		}
		con.chiudiConnessione();
		
		// query 2 - media voto di tutte le domande per partner

		sql = "select distinct " 
				+ " (case when acn.virtual_Chat = 1 then (acn.id_Partner || ' + VA') else acn.id_Partner end)," 
				+ " round(sum(acv.voto * acvd.peso) * 1.0D/sum(acvd.peso),2)" 
				+ " from Accessi_Chat_New acn " 
				+ " join accessi_Chat_Valut_New acv on acv.id_chat = acn.id_chat" 
				+ " join accessi_Chat_Valut_Descr acvd on acv.cod_domanda = acvd.id_domanda"  
				+ " where acn.data_Richiesta_Chat is not null"  
				+ " and acn.id_Partner is not null" 
				+ " and acvd.flg_Attiva = 1 "  
				+ " and acn.data_Richiesta_Chat >= to_date(?, 'dd/MM/yyyy')"  
				+ " and acn.data_Richiesta_Chat <= to_date(?, 'dd/MM/yyyy')"  
				+ " group by (case when acn.virtual_Chat = 1 then (acn.id_Partner || ' + VA') else acn.id_Partner end)";

		ps = con.creaConnessione().prepareStatement(sql);
		ps.setString(1, Utilities.getDateInString(dataInizio, dateFormat));
		ps.setString(2, Utilities.getDateInString(dataFine, dateFormat));
		
		rs = ps.executeQuery();
		
		while(rs.next()) {
			AccessiChatDettagli dettaglio = new AccessiChatDettagli();
			dettaglio.setPartner(rs.getString(1));
			dettaglio.setQp(rs.getString(2));
			for(AccessiChatDettagli dett : dettagliApp) {
				if(dett.getCodDomanda() != null) {
					dettaglio.setPeriodo(Utilities.getDateInString(dataInizio, dateFormat) + " - " + Utilities.getDateInString(dataInizio, dateFormat));
					if (dett.getPartner().equals(dettaglio.getPartner())) {
						switch (dett.getCodDomanda().intValue()) {
							case 1:
								dettaglio.setCortesia(dett.getQp());
								break;
							case 2:
								dettaglio.setEsigenza(dett.getQp());
								break;
							case 3:
								dettaglio.setChiarezza(dett.getQp());
								break;
							case 4:
								dettaglio.setRisProblema(dett.getQp());
								break;
							case 5:
								dettaglio.setTempoAttesa(dett.getQp());
								break;
							case 6:
								dettaglio.setValEsperienza(dett.getQp());
								break;
						}
					}
				}
			}
			//suddivisione del parnter dal servizio per successivo insert
			dettaglio.setServizio(Utilities.getServizio(dettaglio.getPartner()));
			dettaglio.setPartner(Utilities.getPartner(dettaglio.getPartner()));
			
			dettagli.add(dettaglio);
		}
		} catch (SQLException e) {
			log.error("Errore sql: ", e);
		} catch (Exception e) {
			log.error("Errore non gestito: ", e);
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
				if(ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				log.error("Errore durante la chiusura dello statement: ", e);
			}
			con.chiudiConnessione();			
		}

		return dettagli;
	}
}
