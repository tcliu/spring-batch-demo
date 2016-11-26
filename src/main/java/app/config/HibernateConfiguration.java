package app.config;

import app.hibernate.interceptor.SqlInterceptor;
import org.hibernate.Interceptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.JpaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.jta.JtaTransactionManager;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Created by Liu on 10/26/2016.
 */
@Configuration
public class HibernateConfiguration extends HibernateJpaAutoConfiguration {

    public HibernateConfiguration(final DataSource dataSource, final JpaProperties jpaProperties, final ObjectProvider<JtaTransactionManager> jtaTransactionManagerProvider) {
        super(dataSource, jpaProperties, jtaTransactionManagerProvider);
    }

    @Override
    protected void customizeVendorProperties(final Map<String, Object> vendorProperties) {
        vendorProperties.put("hibernate.ejb.interceptor", customInterceptor());
    }

    @Bean
    public Interceptor customInterceptor() {
        return new SqlInterceptor();
    }
}
