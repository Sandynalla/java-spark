package com.optum.mgd.spark.dao.impl;

import java.util.Date;
import java.util.List;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import com.optum.mgd.spark.Idao.IDBUtility;
import com.optum.mgd.spark.util.HibernateUtil;
import com.optum.spark.java.entity.LoadHistory;

public class DBUtility implements IDBUtility {

	private static final String GET_VIEW_QUERY = "SELECT REFERENCED_NAME FROM ALL_DEPENDENCIES WHERE NAME = :viewName AND OWNER = 'ACEUDW' AND REFERENCED_TYPE = 'TABLE'";
	private static final String UPDATE_LOAD_HISTORUY = "update LOADHISTORY set LOADFINISH = :loadFinish, STATUSMESSAGE= :statusMessage,SUCCESS =:success where LOADSTART = :startTime and FILENAME =:fileName and TABLENAME = :tableName";
	@Override
	public String getView(String referenceViewName) {
		Session session = HibernateUtil.getSessionFactory().openSession();
		String viewName = null;

		try {
			session.beginTransaction();
			Query query = session.createSQLQuery(GET_VIEW_QUERY);
			 query.setParameter("viewName", referenceViewName);
			 viewName = query.uniqueResult().toString();
				System.out.println("View Name: " + viewName);

		} catch (HibernateException ex) {
			System.out.println("Exception occured :" + ex);

		} finally {
			session.close();
		}
		return viewName;
	}

	@Override
	public void truncateTable(String tableName) {

		Session session = HibernateUtil.getSessionFactory().openSession();
		try {
			session.beginTransaction();
			Query query = session.createSQLQuery("TRUNCATE TABLE " + tableName);
			query.executeUpdate();
			System.out.println("Truncated Table:" + tableName);

		} catch (HibernateException ex) {
			System.out.println("Exception occured in Truncate method :" + ex);

		} finally {
			session.close();
		}
	}

	public void saveLoadHistroy(String tableName, Date loadStart, Date loadFinish, String statusMessage, String success,
			String fileName, String fileSize) {

		LoadHistory loadHistory = new LoadHistory();
		loadHistory.setTableName(tableName);
		loadHistory.setLoadStart(loadStart);
		loadHistory.setLoadFinish(loadFinish);
		loadHistory.setStatusMessage(statusMessage);
		loadHistory.setSuccess(success);
		loadHistory.setFileName(fileName);
		loadHistory.setFileSize(fileSize);

		Session session = HibernateUtil.getSessionFactory().openSession();
		try {
			session.beginTransaction();
			session.save(loadHistory);
			session.getTransaction().commit();

		} catch (HibernateException ex) {

			System.out.println("Exception occured in while inserting into LoadHistory table :" + ex);
			throw new HibernateException(ex);

		} finally {
			session.close();
		}
	}
	public void updateLoadHistroy(String tableName, Date loadStart, Date loadFinish, String statusMessage, String success,
			String fileName) {

		Session session = HibernateUtil.getSessionFactory().openSession();
		try {
			session.beginTransaction();
			Query query = session.createSQLQuery(UPDATE_LOAD_HISTORUY);
			query.setParameter("tableName", tableName);
			query.setParameter("startTime", loadStart);
			query.setParameter("loadFinish", loadFinish);
			query.setParameter("statusMessage", statusMessage);
			query.setParameter("success", success);
			query.setParameter("fileName", fileName);
			query.executeUpdate();
			session.getTransaction().commit();
		} catch (HibernateException ex) {

			System.out.println("Exception occured in while updating into LoadHistory table :" + ex);
			throw new HibernateException(ex);

		} finally {
			session.close();
		}
	}
}
