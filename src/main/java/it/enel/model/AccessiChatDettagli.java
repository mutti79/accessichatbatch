package it.enel.model;

public class AccessiChatDettagli {

	//PERIODO, PARTNER, SERVIZIO, QP, CORTESIA, ESIGENZA, CHIAREZZA, RISPROBLEMA, TEMPOATTESA, VALESPERIENZA
	private String  periodo;
	private String  partner;
	private String  servizio;
	private String  qp;
	private String  cortesia;
	private String  esigenza;
	private String  chiarezza;
	private String  risProblema;
	private String  tempoAttesa;
	private String  valEsperienza;
	
	private Long codDomanda;
	
	public Long getCodDomanda() {
		return codDomanda;
	}
	public void setCodDomanda(Long codDomanda) {
		this.codDomanda = codDomanda;
	}
	public String getPeriodo() {
		return periodo;
	}
	public void setPeriodo(String periodo) {
		this.periodo = periodo;
	}
	public String getPartner() {
		return partner;
	}
	public void setPartner(String partner) {
		this.partner = partner;
	}
	public String getServizio() {
		return servizio;
	}
	public void setServizio(String servizio) {
		this.servizio = servizio;
	}
	public String getQp() {
		return qp;
	}
	public void setQp(String qp) {
		this.qp = qp;
	}
	public String getCortesia() {
		return cortesia;
	}
	public void setCortesia(String cortesia) {
		this.cortesia = cortesia;
	}
	public String getEsigenza() {
		return esigenza;
	}
	public void setEsigenza(String esigenza) {
		this.esigenza = esigenza;
	}
	public String getChiarezza() {
		return chiarezza;
	}
	public void setChiarezza(String chiarezza) {
		this.chiarezza = chiarezza;
	}
	public String getRisProblema() {
		return risProblema;
	}
	public void setRisProblema(String risProblema) {
		this.risProblema = risProblema;
	}
	public String getTempoAttesa() {
		return tempoAttesa;
	}
	public void setTempoAttesa(String tempoAttesa) {
		this.tempoAttesa = tempoAttesa;
	}
	public String getValEsperienza() {
		return valEsperienza;
	}
	public void setValEsperienza(String valEsperienza) {
		this.valEsperienza = valEsperienza;
	}
	
	
}
