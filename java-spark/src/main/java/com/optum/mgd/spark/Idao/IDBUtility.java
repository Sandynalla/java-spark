package com.optum.mgd.spark.Idao;

public interface IDBUtility {
	public String getView(String referenceViewName);
	public void truncateTable(String tableName);
	
}
