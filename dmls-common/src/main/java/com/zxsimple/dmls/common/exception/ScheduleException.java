package com.zxsimple.dmls.common.exception;

/**
 * Created by zxsimple on 5/13/2016.
 */
public class ScheduleException extends Exception {
    public ScheduleException() {
        super();
    }

    public ScheduleException(String message) {
        super(message);
    }

    public ScheduleException(String message, Throwable cause) {
        super(message, cause);
    }

    public ScheduleException(Throwable cause) {
        super(cause);
    }

    protected ScheduleException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
