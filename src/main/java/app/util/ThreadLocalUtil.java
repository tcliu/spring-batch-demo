package app.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Liu on 10/26/2016.
 */
public class ThreadLocalUtil {

    private static final ThreadLocal<Map<Object,Object>> THREAD_LOCAL_MAP = new ThreadLocal<Map<Object,Object>>() {

        @Override
        protected Map<Object, Object> initialValue() {
            return new HashMap<>();
        }
    };

    public static Map<Object,Object> getContextMap() {
        return THREAD_LOCAL_MAP.get();
    }

    public static Object put(Object key, Object value) {
        return getContextMap().put(key, value);
    }

    public static Object get(Object key) {
        return getContextMap().get(key);
    }

    public static void remove(Object key) {
        getContextMap().remove(key);
    }

    public static boolean containsKey(Object key) {
        return getContextMap().containsKey(key);
    }
}
