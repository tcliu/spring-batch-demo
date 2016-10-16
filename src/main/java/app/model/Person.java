package app.model;

import org.hibernate.annotations.GenericGenerator;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import javax.persistence.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "person", uniqueConstraints = @UniqueConstraint(name = "ux_person_name", columnNames = {"first_name", "last_name"}))
public class Person implements CrudEntity<String> {

    @Id
    @Column(name = "person_id", columnDefinition = "VARCHAR(36)", nullable = false)
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid", strategy = "uuid2")
    private String id;

    @Column(name = "last_name", length = 100, columnDefinition = "NVARCHAR2(100)", nullable = false)
    private String lastName;

    @Column(name = "first_name", length = 100, columnDefinition = "NVARCHAR2(100)", nullable = false)
    private String firstName;

    @Column(name = "date_of_birth", columnDefinition = "DATE", nullable = false)
    private LocalDate dateOfBirth;

    @Column(name = "nationality", columnDefinition = "VARCHAR2(30)")
    private String nationalityCode;

    @ManyToOne
    @JoinColumn(name = "nationality", updatable = false, insertable = false, foreignKey = @ForeignKey(name = "fk_person_nationality"))
    private Nationality nationality;

    @CreatedDate
    @Column(name = "created", columnDefinition = "TIMESTAMP")
    private LocalDateTime created;

    @LastModifiedDate
    @Column(name = "last_updated", columnDefinition = "TIMESTAMP")
    private LocalDateTime lastUpdated;

    public Person() {

    }

    public Person(String id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(final String id) {
        this.id = id;
    }

    public void beforeSave() {
        if (getCreated() == null) {
            setCreated(LocalDateTime.now());
        }
        setLastUpdated(LocalDateTime.now());
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public LocalDate getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(final LocalDate dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public String getNationalityCode() {
        return nationalityCode;
    }

    public void setNationalityCode(final String nationality) {
        this.nationalityCode = nationality;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public void setCreated(final LocalDateTime created) {
        this.created = created;
    }

    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(final LocalDateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id='" + id + '\'' +
                ", lastName='" + lastName + '\'' +
                ", firstName='" + firstName + '\'' +
                ", dateOfBirth=" + dateOfBirth +
                ", nationality='" + nationality + '\'' +
                ", created=" + created +
                ", lastUpdated=" + lastUpdated +
                '}';
    }
}