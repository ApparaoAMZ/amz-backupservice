package com.amazon.gdpr.util;

public class GdprException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	String exceptionMessage;

	/**
	 * @param exceptionMessage
	 */
	public GdprException(String exceptionMessage) {
		super();
		this.exceptionMessage = exceptionMessage;
	}
	/**
	 * @return the exceptionMessage
	 */
	public String getExceptionMessage() {
		return exceptionMessage;
	}

	/**
	 * @param exceptionMessage the exceptionMessage to set
	 */
	public void setExceptionMessage(String exceptionMessage) {
		this.exceptionMessage = exceptionMessage;
	}

}