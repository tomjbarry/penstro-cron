package com.py.cron.mongo.archive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.py.cron.constants.ArchiveCollectionNames;
import com.py.cron.exception.JobException;
import com.py.py.constants.ArchivalTimes;
import com.py.py.domain.Event;
import com.py.py.domain.constants.CollectionNames;
import com.py.py.util.PyUtils;

public class EventsArchiveJob extends ArchiveJob {

	public EventsArchiveJob() throws JobException {
		super();
	}

	@Override
	protected String getCollectionName() {
		return CollectionNames.EVENT;
	}
	
	@Override
	protected String getArchiveCollectionName() {
		return ArchiveCollectionNames.EVENT;
	}

	@Override
	protected DBObject getReadQuery() throws JobException {
		return new BasicDBObject(Event.CREATED, new BasicDBObject("$lt", PyUtils.getOldDate(ArchivalTimes.EVENT)));
	}

	@Override
	protected DBObject getIdQuery(DBObject obj) throws JobException {
		return new BasicDBObject(Event.ID, obj.get(Event.ID));
	}

}
