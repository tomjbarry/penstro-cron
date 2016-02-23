package com.py.cron.mongo.archive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.py.cron.constants.ArchiveCollectionNames;
import com.py.cron.exception.JobException;
import com.py.py.constants.ArchivalTimes;
import com.py.py.domain.AdminAction;
import com.py.py.domain.constants.CollectionNames;
import com.py.py.enumeration.ADMIN_STATE;
import com.py.py.util.PyUtils;

public class AdminActionsArchiveJob extends ArchiveJob {

	public AdminActionsArchiveJob() throws JobException {
		super();
	}

	@Override
	protected String getCollectionName() {
		return CollectionNames.ADMIN;
	}

	@Override
	protected String getArchiveCollectionName() {
		return ArchiveCollectionNames.ADMIN;
	}

	@Override
	protected DBObject getReadQuery() throws JobException {
		DBObject query = new BasicDBObject(AdminAction.CREATED, new BasicDBObject("$lt", PyUtils.getOldDate(ArchivalTimes.ADMIN)));
		query.put(AdminAction.STATE, ADMIN_STATE.COMPLETE.toString());
		return query;
	}

	@Override
	protected DBObject getIdQuery(DBObject obj) throws JobException {
		return new BasicDBObject(AdminAction.ID, obj.get(AdminAction.ID));
	}

}
