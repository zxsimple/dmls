package com.zxsimple.dmls.common.exception;

public class MailException extends RuntimeException {

	public MailException() {
	}

	public MailException(String message) {
		super(message);
	}

	public MailException(String message, Throwable cause) {
		super(message, cause);
	}

	public MailException(Throwable cause) {
		super(cause);
	}
    
}
