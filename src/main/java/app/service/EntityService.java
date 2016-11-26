package app.service;

import app.exception.ApplicationException;
import app.model.CrudEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by Liu on 10/15/2016.
 */
public class EntityService<T extends CrudEntity<ID>,ID extends Serializable> {

    @Autowired
    private JpaRepository<T,ID> jpaRepository;

    @PersistenceContext
    private EntityManager entityManager;

    public JpaRepository<T,ID> getJpaRepository() {
        return jpaRepository;
    }

    public T get(ID id) {
        return jpaRepository.findOne(id);
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public List<T> create(T... entities) {
        return create(Arrays.asList(entities));
    }

    @Transactional(rollbackFor = Throwable.class)
    public <S extends T> List<S> create(Stream<S> entities) {
        return update(entities.collect(Collectors.toList()));
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Throwable.class)
    public <S extends T> List<S> create(Iterable<S> entities) {
        List<S> created = null;
        if (entities != null) {
            entities.forEach(entity -> {
                if (entity.getId() != null) {
                    throw new ApplicationException("ID should not be provided on create.");
                }
            });
            try {
                created = jpaRepository.save(entities);
                // reset id to null
                entityManager.flush();
            } catch (RuntimeException e) {
                entities.forEach(entity -> entity.setId(null));
                throw e;
            }
        }
        return created;
    }

    @Transactional(rollbackFor = Throwable.class)
    public List<T> update(T... entities) {
        return update(Arrays.asList(entities));
    }

    @Transactional(rollbackFor = Throwable.class)
    public <S extends T> List<S> update(Stream<S> entities) {
        return update(entities.collect(Collectors.toList()));
    }

    @Transactional(rollbackFor = Throwable.class)
    public <S extends T> List<S> update(Iterable<S> entities) {
        List<S> updated = null;
        if (entities != null) {
            entities.forEach(entity -> {
                if (entity.getId() == null) {
                    throw new ApplicationException("ID should be provided on update.");
                } else if (!jpaRepository.exists(entity.getId())) {
                    throw new ApplicationException("Entity identified by " + entity.getId() + " does not exist.");
                }
            });
            updated = jpaRepository.save(entities);
            entityManager.flush();
        }
        return updated;
    }

    @Transactional(rollbackFor = Throwable.class)
    public List<T> save(T... entities) {
        return save(Arrays.asList(entities));
    }

    @Transactional(rollbackFor = Throwable.class)
    public <S extends T> List<S> save(Stream<S> entities) {
        return update(entities.collect(Collectors.toList()));
    }

    @Transactional(rollbackFor = Throwable.class)
    public <S extends T> List<S> save(Iterable<S> entities) {
        Map<Boolean,List<S>> m = StreamSupport.stream(entities.spliterator(), false).collect(Collectors.partitioningBy(e -> e.getId() == null));
        List<S> l = new ArrayList<>();
        l.addAll(update(m.get(false)));
        l.addAll(create(m.get(true)));
        return l;
    }

    @Transactional(rollbackFor = Throwable.class)
    public void deleteAll() {
        getJpaRepository().deleteAll();
    }
}
