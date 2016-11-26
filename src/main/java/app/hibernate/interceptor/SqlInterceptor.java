package app.hibernate.interceptor;

import app.util.ThreadLocalUtil;
import org.hibernate.EmptyInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

/**
 * Created by Liu on 10/26/2016.
 */
public class SqlInterceptor extends EmptyInterceptor {

    @Autowired
    private ApplicationContext applicationContext;

    @Override
    public String onPrepareStatement(String sql) {
        if (ThreadLocalUtil.containsKey("table_name")) {
            sql = sql.replace("person", (String) ThreadLocalUtil.get("table_name"));
        }
        return super.onPrepareStatement(sql);
    }

}
