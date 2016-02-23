package com.py.cron.mongo.archive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.py.cron.constants.ArchiveCollectionNames;
import com.py.cron.exception.JobException;
import com.py.py.constants.ArchivalTimes;
import com.py.py.domain.Comment;
import com.py.py.domain.constants.CollectionNames;
import com.py.py.util.PyUtils;

public class CommentsArchiveJob extends ArchiveJob {

	public CommentsArchiveJob() throws JobException {
		super();
	}

	@Override
	protected String getCollectionName() {
		return CollectionNames.COMMENT;
	}

	@Override
	protected String getArchiveCollectionName() {
		return ArchiveCollectionNames.COMMENT;
	}

	@Override
	protected DBObject getReadQuery() throws JobException {
		return new BasicDBObject(Comment.ARCHIVED, new BasicDBObject("$lt", PyUtils.getOldDate(ArchivalTimes.COMMENT)));
	}

	@Override
	protected DBObject getIdQuery(DBObject obj) throws JobException {
		return new BasicDBObject(Comment.ID, obj.get(Comment.ID));
	}
}
