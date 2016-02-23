package com.py.cron.mongo.remove;

import com.mongodb.DBObject;
import com.py.cron.exception.JobException;
import com.py.cron.mongo.MongoJob;

public abstract class RemoveJob extends MongoJob {
	
	public RemoveJob() throws JobException {
		super();
	}
	
	protected abstract DBObject getRemoveQuery() throws JobException;
	
	protected void remove() throws JobException {
		try {
			col.remove(getRemoveQuery());
		} catch(Exception e) {
			throw new JobException(e);
		}
	}
	
	public void run() throws JobException {
		super.run();
		
		try {
			remove();
		} catch(Exception e) {
			throw new JobException(e);
		}
	}

}
