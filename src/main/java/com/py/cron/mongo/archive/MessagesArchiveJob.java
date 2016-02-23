package com.py.cron.mongo.archive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.py.cron.constants.ArchiveCollectionNames;
import com.py.cron.exception.JobException;
import com.py.py.constants.ArchivalTimes;
import com.py.py.domain.Message;
import com.py.py.domain.constants.CollectionNames;
import com.py.py.util.PyUtils;

public class MessagesArchiveJob extends ArchiveJob {

	public MessagesArchiveJob() throws JobException {
		super();
	}

	@Override
	protected String getCollectionName() {
		return CollectionNames.MESSAGE;
	}
	
	@Override
	protected String getArchiveCollectionName() {
		return ArchiveCollectionNames.MESSAGE;
	}

	@Override
	protected DBObject getReadQuery() throws JobException {
		return new BasicDBObject(Message.CREATED, new BasicDBObject("$lt", PyUtils.getOldDate(ArchivalTimes.MESSAGE)));
	}

	@Override
	protected DBObject getIdQuery(DBObject obj) throws JobException {
		return new BasicDBObject(Message.ID, obj.get(Message.ID));
	}

}
