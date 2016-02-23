package com.py.cron.job;

import com.py.cron.exception.JobException;

public interface Job {

	void run() throws JobException;
}
