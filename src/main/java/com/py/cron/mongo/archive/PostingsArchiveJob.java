package com.py.cron.mongo.archive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.py.cron.constants.ArchiveCollectionNames;
import com.py.cron.exception.JobException;
import com.py.py.constants.ArchivalTimes;
import com.py.py.domain.Posting;
import com.py.py.domain.constants.CollectionNames;
import com.py.py.util.PyUtils;

public class PostingsArchiveJob extends ArchiveJob {

	public PostingsArchiveJob() throws JobException {
		super();
	}

	@Override
	protected String getCollectionName() {
		return CollectionNames.POSTING;
	}

	@Override
	protected String getArchiveCollectionName() {
		return ArchiveCollectionNames.POSTING;
	}

	@Override
	protected DBObject getReadQuery() throws JobException {
		return new BasicDBObject(Posting.ARCHIVED, new BasicDBObject("$lt", PyUtils.getOldDate(ArchivalTimes.POSTING)));
	}

	@Override
	protected DBObject getIdQuery(DBObject obj) throws JobException {
		return new BasicDBObject(Posting.ID, obj.get(Posting.ID));
	}

}
