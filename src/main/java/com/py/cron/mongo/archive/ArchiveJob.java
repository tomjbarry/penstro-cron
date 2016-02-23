package com.py.cron.mongo.archive;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.py.cron.constants.CronValues;
import com.py.cron.constants.PropertyNames;
import com.py.cron.constants.SystemProperties;
import com.py.cron.exception.JobException;
import com.py.cron.mongo.MongoJob;
import com.py.py.util.PyLogger;
import com.py.py.util.PyUtils;

public abstract class ArchiveJob extends MongoJob {
	protected static final PyLogger logger = PyLogger.getLogger(ArchiveJob.class);
	
	protected static MongoClient archive;
	protected static MongoTemplate archiveTemplate;
	protected static DBCollection archiveCol;
	
	private static boolean initialized = false;
	
	private static void initialize() throws JobException {
		if(!initialized) {
			String address1 = properties.getProperty(PropertyNames.MONGO_ARCHIVE_ADDRESS_1);
			String address2 = properties.getProperty(PropertyNames.MONGO_ARCHIVE_ADDRESS_2);
			String address3 = properties.getProperty(PropertyNames.MONGO_ARCHIVE_ADDRESS_3);
			String port1 = properties.getProperty(PropertyNames.MONGO_ARCHIVE_PORT_1);
			String port2 = properties.getProperty(PropertyNames.MONGO_ARCHIVE_PORT_2);
			String port3 = properties.getProperty(PropertyNames.MONGO_ARCHIVE_PORT_3);
			String database = properties.getProperty(PropertyNames.MONGO_ARCHIVE_DATABASE_NAME);
			String credentialsUsername = properties.getProperty(PropertyNames.MONGO_ARCHIVE_CREDENTIALS_USERNAME);
			String credentialsPassword = properties.getProperty(PropertyNames.MONGO_ARCHIVE_CREDENTIALS_PASSWORD);
			String credentialsDatabase = properties.getProperty(PropertyNames.MONGO_ARCHIVE_CREDENTIALS_DATABASE);
			
			String ca = properties.getProperty(PropertyNames.MONGO_CA_PATH);
			String caPass = properties.getProperty(PropertyNames.MONGO_CA_PASSWORD);
			String pem = properties.getProperty(PropertyNames.MONGO_PEM_PATH);
			String pemPass = properties.getProperty(PropertyNames.MONGO_PEM_PASSWORD);
			
			try {
				if(database == null) {
					throw new JobException("Archive database name '" + PropertyNames.MONGO_ARCHIVE_DATABASE_NAME + "' must be provided!");
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
						archive = new MongoClient(addresses, credentials, options);
					} else {
						archive = new MongoClient(addresses.get(0), credentials, options);
					}
				} else {
					archive = new MongoClient(new ServerAddress(), credentials, options);
				}
				archiveTemplate = new MongoTemplate(archive, database);
				initialized = true;
			} catch(Exception e) {
				throw new JobException(e);
			}
		}
	}
	
	public ArchiveJob() throws JobException {
		super();
		initialize();
	}
	
	public void run() throws JobException {
		super.run();
		if(archive == null) {
			throw new JobException("Error connecting to archive!");
		}
		
		logger.info("Starting job of type: " + this.getClass());

		String archiveColName = getArchiveCollectionName();
		archiveCol = archiveTemplate.getCollection(archiveColName);
		if(PyUtils.stringCompare(archiveTemplate.getDb().getName(), template.getDb().getName()) 
				&& PyUtils.stringCompare(archiveCol.getFullName(), col.getFullName())) {
			throw new JobException("Same database and collection! This could possibly overwrite data! Aborting!");
		}

		try {
			archive();
		} catch(Exception e) {
			throw new JobException(e);
		}
		logger.info("Completed job of type: " + this.getClass());
	}
	
	protected void archive() throws JobException {
		Page<DBObject> list = read();

		if(list == null) {
			throw new JobException("Read was unsuccessful.");
		}
		
		logger.info("Read (" + list.getNumberOfElements() + ") documents");
		
		if(list.getNumberOfElements() == 0) {
			// nothing to do here, continue to other jobs
			logger.info("Skipping write and finalize steps.");
			return;
		}
		
		write(list.getContent());
		finalize(list.getContent());
	}
	
	protected int getBatchSize() {
		return CronValues.CRON_BATCH_SIZE;
	}
	
	protected abstract String getArchiveCollectionName();
	protected abstract DBObject getReadQuery() throws JobException;
	protected abstract DBObject getIdQuery(DBObject obj) throws JobException;
	
	public Page<DBObject> read() throws JobException {
		Pageable pageable = new PageRequest(0, getBatchSize());
		
		DBObject query = getReadQuery();
		try {
			DBCursor cursor = col.find(query)
								.skip(pageable.getOffset())
								.limit(pageable.getPageSize());
			
			if(cursor == null) {
				throw new NullPointerException();
			}
			
			List<DBObject> list = new ArrayList<DBObject>();
			
			while(cursor.hasNext()) {
				list.add(cursor.next());
			}
			
			return new PageImpl<DBObject>(list, pageable, cursor.count());
		} catch(Exception e) {
			throw new JobException(e);
		}
	}
	
	public void write(List<DBObject> list) throws JobException {
		if(list == null) {
			throw new NullPointerException();
		}
		
		try {
			archiveCol.insert(list);
		} catch(Exception e) {
			throw new JobException(e);
		}
		
		logger.info("Wrote (" + list.size() + ") documents");
	}
	
	protected void removeArchived(DBObject obj) throws JobException {
		archiveCol.remove(getIdQuery(obj));
	}
	
	protected void removeOriginal(DBObject obj) throws JobException {
		col.remove(getIdQuery(obj));
	}
	
	public void finalize(List<DBObject> list) throws JobException {
		if(list == null) {
			throw new NullPointerException();
		}
		
		Exception lastException = null;
		long countArchived = 0;
		long countOriginal = 0;
		
		for(DBObject obj : list) {
			try {
				if(obj == null) {
					throw new JobException("Null value in list to finalize!");
				}
				DBObject foundArchived = archiveCol.findOne(obj);
				DBObject foundOriginal = col.findOne(obj);
				if(foundArchived == null || foundOriginal == null) {
					// delete the archived one by key
					removeArchived(obj);
					countArchived++;
				} else {
					// delete the original by key
					removeOriginal(obj);
					countOriginal++;
				}
			} catch(Exception e) {
				lastException = e;
			}
		}
		
		logger.info("Finialized (" + list.size() + ") documents, with (" + countArchived + ") archivals cancelled and (" + countOriginal + ") successfully completed.");
		
		if(lastException != null) {
			throw new JobException(lastException);
		}
		
	}
}
