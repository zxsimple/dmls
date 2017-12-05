package com.zxsimple.dmls.common.exception;

/**
 * Created by zxsimple on 2016/7/11.
 */
public class ModelException extends Exception {

    public ModelException() {
        super();
    }

    public ModelException(String message) {
        super(message);
    }

    public ModelException(String message, Throwable cause) {
        super(message, cause);
    }

    public ModelException(Throwable cause) {
        super(cause);
    }

    protected ModelException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
