package app.exception;

/**
 * Created by Liu on 10/15/2016.
 */
public class RetryableException extends Exception {

    public RetryableException() {
    }

    public RetryableException(final String message) {
        super(message);
    }

    public RetryableException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RetryableException(final Throwable cause) {
        super(cause);
    }
}
