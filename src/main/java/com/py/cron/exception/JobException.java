package com.py.cron.exception;

public class JobException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6688270222953550192L;

	public JobException() {
		super();
	}
	
	public JobException(String message) {
		super(message);
	}
	
	public JobException(Exception e) {
		super(e);
	}
	
	public JobException(String message, Exception e) {
		super(message, e);
	}
}
