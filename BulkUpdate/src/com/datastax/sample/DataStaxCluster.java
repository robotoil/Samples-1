package com.datastax.sample;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;

public class DataStaxCluster {
	private String node;
	private String keyspace;
	private Cluster cluster;
	private Session session;
	
	//prepared statements
	private PreparedStatement psGetDocuments;
	private PreparedStatement psUpdateLanguage;
	
	
	public String getNode() {
		return node;
	}

	public void setNode(String node) {
		this.node = node;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public DataStaxCluster(String node, String keyspace){
		setNode(node);
		setKeyspace(keyspace);
		
		connect();
		prepare();
	}
	
	private void connect() {
		Builder builder = Cluster.builder();
		builder.addContactPoints(node);
		//builder.withCredentials("cassandra", "password");
		cluster = builder.build();
		cluster.getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM);
		//cluster.getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
		session = cluster.connect(keyspace);
	}
	
	
	private void prepare(){
		psGetDocuments = session.prepare("select cust_face_acct_nb, doc_id, doc_id_appl_sys_cpnt_id, doc_cre_ts, language_cd from docs_by_account where token(cust_face_acct_nb) >= ? and token(cust_face_acct_nb) <= ?");
		psUpdateLanguage = session.prepare("insert into docs_by_account(cust_face_acct_nb, doc_id, doc_id_appl_sys_cpnt_id, doc_cre_ts, language_cd) values(?, ?, ?, ?, ?)");
	}	
	
	
	public void updateLanguageCode(String newLanguageCd){
		//Set the batch size
		int MAX_BATCH_SIZE = 99; //keep batches small for best peformance
		//Break up the updates into token ranges..
		long[] tokens = {-9223372036854775808L, -4611686018427387904L, 0L, 4611686018427387904L, 9223372036854775807L};

		//Loop through each token range
		for (int i=0; i<tokens.length-2; i++){
			BatchStatement bs = new BatchStatement(BatchStatement.Type.UNLOGGED);
			String cust_face_acct_nb = "";
			int batchSize = 0;
			
			ResultSet results = session.execute(psGetDocuments.bind(tokens[i], tokens[i+1]));

			for (Row row : results ){
				DocByAccount document = new DocByAccount(row);
				
				//set the current partition key if empty
				if (cust_face_acct_nb.equals("")){
					cust_face_acct_nb = document.getCust_face_acct_nb();
				}
			
				//Execute batch if batchSize > MAX_BATCH_SIZE or if partition key has changed
				if (!document.getCust_face_acct_nb().equals(cust_face_acct_nb) || batchSize > MAX_BATCH_SIZE){
					ResultSetFuture future = session.executeAsync(bs); 
					//TODO: Add a callback to verify that batch succeeded
					//See: http://www.datastax.com/dev/blog/java-driver-async-queries
					
					batchSize=0;
					cust_face_acct_nb = "";
					//Reset the batch
					bs.clear();
				}
				
				//If the language code needs to be updated, add this document to the batch
				if (!document.getLanguage_cd().equals(newLanguageCd)){
					cust_face_acct_nb = document.getCust_face_acct_nb();
					bs.add(psUpdateLanguage.bind(document.getCust_face_acct_nb(), document.getDoc_id(), document.getDoc_id_appl_sys_cpnt_id(), document.getDoc_cre_ts(), newLanguageCd));
				}				
			}
			
			//Execute final batch
			if (batchSize>0){
				ResultSetFuture future = session.executeAsync(bs); //TODO: Add a callback to verify that batch succeeded
			}
			
		}
	}
	
}
