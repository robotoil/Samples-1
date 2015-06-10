package com.datastax.sample;

import java.util.Date;

import com.datastax.driver.core.Row;

public class DocByAccount {
	private String cust_face_acct_nb;
	private String doc_id;
	private String doc_id_appl_sys_cpnt_id;
	private Date doc_cre_ts;
	private String language_cd;
	
	public DocByAccount(Row row){
		this.setCust_face_acct_nb(row.getString("cust_face_acct_nb"));
		this.setDoc_cre_ts(row.getDate("doc_cre_ts"));
		this.setDoc_id(row.getString("doc_id"));
		this.setDoc_id_appl_sys_cpnt_id(row.getString("doc_id_appl_sys_cpnt_id"));
		this.setLanguage_cd(row.getString("language_cd"));
	}
	
	public String getCust_face_acct_nb() {
		return cust_face_acct_nb;
	}
	public void setCust_face_acct_nb(String cust_face_acct_nb) {
		this.cust_face_acct_nb = cust_face_acct_nb;
	}
	public String getDoc_id() {
		return doc_id;
	}
	public void setDoc_id(String doc_id) {
		this.doc_id = doc_id;
	}
	public String getDoc_id_appl_sys_cpnt_id() {
		return doc_id_appl_sys_cpnt_id;
	}
	public void setDoc_id_appl_sys_cpnt_id(String doc_id_appl_sys_cpnt_id) {
		this.doc_id_appl_sys_cpnt_id = doc_id_appl_sys_cpnt_id;
	}
	public Date getDoc_cre_ts() {
		return doc_cre_ts;
	}
	public void setDoc_cre_ts(Date doc_cre_ts) {
		this.doc_cre_ts = doc_cre_ts;
	}
	public String getLanguage_cd() {
		return language_cd;
	}
	public void setLanguage_cd(String language_cd) {
		this.language_cd = language_cd;
	}
	
}
