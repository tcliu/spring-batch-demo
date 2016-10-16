package app.exception;

/**
 * Created by Liu on 10/15/2016.
 */
public class ApplicationException extends RuntimeException {

    public ApplicationException() {
        super();
    }

    public ApplicationException(final String message) {
        super(message);
    }

    public ApplicationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ApplicationException(final Throwable cause) {
        super(cause);
    }
}
