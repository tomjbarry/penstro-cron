package com.py.cron.mongo;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.py.cron.constants.PropertyNames;
import com.py.cron.constants.SystemProperties;
import com.py.cron.exception.JobException;
import com.py.cron.job.Job;

public abstract class MongoJob implements Job {
	
	protected static Properties properties;
	protected static MongoClient mongo;
	protected static MongoTemplate template;
	protected static DBCollection col;

	private static boolean initialized = false;
	private static void initialize() throws JobException {
		if(!initialized) {
			properties = new Properties();
			String propFileName = System.getProperty(SystemProperties.CONFIG_PATH);
			try {
				InputStream inputStream = new FileInputStream(propFileName);
				if(inputStream != null) {
					properties.load(inputStream);
					inputStream.close();
				}
			} catch(Exception e) {
				throw new JobException(e);
			}
			
			String address1 = properties.getProperty(PropertyNames.MONGO_ADDRESS_1);
			String address2 = properties.getProperty(PropertyNames.MONGO_ADDRESS_2);
			String address3 = properties.getProperty(PropertyNames.MONGO_ADDRESS_3);
			String port1 = properties.getProperty(PropertyNames.MONGO_PORT_1);
			String port2 = properties.getProperty(PropertyNames.MONGO_PORT_2);
			String port3 = properties.getProperty(PropertyNames.MONGO_PORT_3);
			String database = properties.getProperty(PropertyNames.MONGO_DATABASE_NAME);
			String credentialsUsername = properties.getProperty(PropertyNames.MONGO_CREDENTIALS_USERNAME);
			String credentialsPassword = properties.getProperty(PropertyNames.MONGO_CREDENTIALS_PASSWORD);
			String credentialsDatabase = properties.getProperty(PropertyNames.MONGO_CREDENTIALS_DATABASE);
			
			String ca = properties.getProperty(PropertyNames.MONGO_CA_PATH);
			String caPass = properties.getProperty(PropertyNames.MONGO_CA_PASSWORD);
			String pem = properties.getProperty(PropertyNames.MONGO_PEM_PATH);
			String pemPass = properties.getProperty(PropertyNames.MONGO_PEM_PASSWORD);
			
			try {
				if(database == null) {
					throw new JobException("Database name '" + PropertyNames.MONGO_DATABASE_NAME + "' must be provided!");
				}
				
				List<MongoCredential> credentials = new ArrayList<MongoCredential>();
				if(credentialsUsername != null && credentialsPassword != null && credentialsDatabase != null) {
					credentials.add(MongoCredential.createCredential(credentialsUsername, credentialsDatabase, credentialsPassword.toCharArray()));
				}
				
				MongoClientOptions.Builder builder = MongoClientOptions.builder();
				//if(ca != null && pem != null) {
				if(ca != null && pem != null) {
					System.setProperty(SystemProperties.SSL_KEYSTORE, pem);
					System.setProperty(SystemProperties.SSL_KEYSTORE_PASSWORD, pemPass);
					System.setProperty(SystemProperties.SSL_TRUSTSTORE, ca);
					System.setProperty(SystemProperties.SSL_TRUSTSTORE_PASSWORD, caPass);
					
					builder.sslEnabled(true);
				}
				MongoClientOptions options = builder.build();
				
				if(address1 != null) {
					List<ServerAddress> addresses = new ArrayList<ServerAddress>();
					if(port1 != null) {
						addresses.add(new ServerAddress(address1, Integer.parseInt(port1)));
					} else {
						addresses.add(new ServerAddress(address1));
					}
					if(address2 != null) {
						if(port2 != null) {
							addresses.add(new ServerAddress(address2, Integer.parseInt(port2)));
						} else {
							addresses.add(new ServerAddress(address2));
						}
						if(address3 != null) {
							if(port3 != null) {
								addresses.add(new ServerAddress(address3, Integer.parseInt(port3)));
							} else {
								addresses.add(new ServerAddress(address3));
							}
						}
						mongo = new MongoClient(addresses, credentials, options);
					} else {
						mongo = new MongoClient(addresses.get(0), credentials, options);
					}
				} else {
					mongo = new MongoClient(new ServerAddress(), credentials, options);
				}
				template = new MongoTemplate(mongo, database);
				initialized = true;
			} catch(Exception e) {
				throw new JobException(e);
			}
		}
	}
	
	public MongoJob() throws JobException {
		initialize();
	}
	
	protected abstract String getCollectionName();
	
	public void run() throws JobException {
		if(mongo == null) {
			throw new JobException("Error connecting to database!");
		}
		String colName = getCollectionName();
		col = template.getCollection(colName);
	}
	
}
