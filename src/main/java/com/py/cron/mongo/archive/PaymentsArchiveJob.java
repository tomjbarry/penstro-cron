package com.py.cron.mongo.archive;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.py.cron.constants.ArchiveCollectionNames;
import com.py.cron.exception.JobException;
import com.py.py.constants.ArchivalTimes;
import com.py.py.domain.Payment;
import com.py.py.domain.constants.CollectionNames;
import com.py.py.domain.enumeration.PAYMENT_STATE;
import com.py.py.util.PyUtils;

public class PaymentsArchiveJob extends ArchiveJob {

	public PaymentsArchiveJob() throws JobException {
		super();
	}

	@Override
	protected String getCollectionName() {
		return CollectionNames.PAYMENT;
	}
	
	@Override
	protected String getArchiveCollectionName() {
		return ArchiveCollectionNames.PAYMENT;
	}

	@Override
	protected DBObject getReadQuery() throws JobException {
		DBObject query = new BasicDBObject(Payment.CREATED, new BasicDBObject("$lt", PyUtils.getOldDate(ArchivalTimes.PAYMENT)));
		query.put(Payment.STATE, PAYMENT_STATE.COMPLETED.toString());
		return query;
	}

	@Override
	protected DBObject getIdQuery(DBObject obj) throws JobException {
		return new BasicDBObject(Payment.ID, obj.get(Payment.ID));
	}

}
