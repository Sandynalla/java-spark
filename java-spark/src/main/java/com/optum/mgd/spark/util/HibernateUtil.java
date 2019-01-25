package com.optum.mgd.spark.util;


import java.util.Properties;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

public class HibernateUtil {
	
	public static Properties propertyLoad() {
        Properties properties = null;
            properties = new Properties();
            try {
                properties.load(HibernateUtil.class.getClassLoader()
                        .getResourceAsStream("dbconnections.properties"));
                 } catch (Exception e) {
                e.printStackTrace();
            }
           
        return properties;
    }

	public static SessionFactory getSessionFactory() {
        Properties properties = propertyLoad();

		Configuration configuration = new Configuration().configure("hibernate.cfg.xml").addProperties(properties);

		StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder()
				.applySettings(configuration.getProperties());
		SessionFactory sessionFactory = configuration.buildSessionFactory(builder.build());
		return sessionFactory;

	}
}
