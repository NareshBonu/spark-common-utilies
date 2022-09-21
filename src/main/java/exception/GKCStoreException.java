package exception;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

public class GKCStoreException extends Exception{
    
    private static String GKCStoreErrorCode;
    private static String GKCStoreErrorMessage;

    public GKCStoreException(String message) {
        super(message);
    }

    public GKCStoreException(String message, Throwable cause) {
        super(message,cause);
    }


}
