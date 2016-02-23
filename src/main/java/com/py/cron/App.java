package com.py.cron;

import java.util.ArrayList;
import java.util.List;

import com.py.cron.exception.JobException;
import com.py.cron.job.Job;
import com.py.cron.mongo.archive.AdminActionsArchiveJob;
import com.py.cron.mongo.archive.CommentsArchiveJob;
import com.py.cron.mongo.archive.EventsArchiveJob;
import com.py.cron.mongo.archive.MessagesArchiveJob;
import com.py.cron.mongo.archive.PaymentsArchiveJob;
import com.py.cron.mongo.archive.PostingsArchiveJob;
import com.py.py.util.PyLogger;

public class App 
{
	protected static final PyLogger logger = PyLogger.getLogger(App.class);
	
	private static List<Job> createJobs(String[] args) throws JobException {
		List<Job> jobs = new ArrayList<Job>();
		
		jobs.add(new AdminActionsArchiveJob());
		jobs.add(new PaymentsArchiveJob());
		jobs.add(new PostingsArchiveJob());
		jobs.add(new CommentsArchiveJob());
		jobs.add(new EventsArchiveJob());
		jobs.add(new MessagesArchiveJob());
		
		return jobs;
	}
	
    public static void main( String[] args )
    {
    	try {
	    	List<Job> jobs = createJobs(args);
	    	if(jobs != null) {
	    		for(Job job : jobs) {
	    			try {
	    				job.run();
	    			} catch(JobException je) {
	    				logger.error(je);
	    			}
	    		}
	    		logger.info("Finished all jobs!");
	    	}
    	} catch(JobException je) {
    		logger.error(je);
    	}
    }
}
