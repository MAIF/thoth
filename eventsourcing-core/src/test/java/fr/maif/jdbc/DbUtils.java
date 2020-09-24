package fr.maif.jdbc;

import fr.maif.jdbc.exceptions.ExceptionsHandler;
import fr.maif.jdbc.exceptions.SqlException;
import org.h2.jdbcx.JdbcConnectionPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by adelegue on 13/06/16.
 */
public interface DbUtils {

	AtomicInteger dbNumber = new AtomicInteger();

	DbUtils INSTANCE = new DbUtils() {};

	static String nextDbUrl() {
		return "jdbc:h2:mem:test" + dbNumber.getAndIncrement() + ";DB_CLOSE_DELAY=-1;MODE=Oracle";
	}

	DataSource dataSource = JdbcConnectionPool.create(nextDbUrl(), "user", "password");

	static DataSource getDataSource() {
		return dataSource;
	}

	default void dropAll(Integer inc) {
		Connection c = ExceptionsHandler.handleChecked(dataSource::getConnection);
		try {
			c.prepareStatement("drop table bands"+inc+" if exists ").execute();
		} catch (SQLException e) {

		} finally {
			try {
				c.close();
			} catch (SQLException e) {

			}
		}
	}

	default void createDatabase(Integer inc) {
		dropAll(inc);
		Connection c = ExceptionsHandler.handleChecked(dataSource::getConnection);
		try {
			c.setAutoCommit(true);
			c.prepareStatement("create table bands"+inc+"(band_id int primary key, name varchar(100) not null, createdAt TIMESTAMP )").execute();
			c.prepareStatement("insert into  bands"+inc+"(band_id, name, createdAt) values(1, 'Envy' , '2012-09-17 18:47:52.100')").execute();
			c.prepareStatement("insert into  bands"+inc+"(band_id, name, createdAt) values(2,'At the drive in', '2012-09-17 18:47:52.100')").execute();
			c.prepareStatement("insert into  bands"+inc+"(band_id, name, createdAt) values(3,'Explosion in the sky', '2012-09-17 18:47:52.100')").execute();
		} catch (SQLException e) {

		} finally {
			try {
				c.close();
			} catch (SQLException e) {

			}
		}
	}

	class Band {
		public final Integer id;
		public final String name;
		public final Date createdAt;

		public Band(Integer id, String name, Date createdAt) {
			this.id = id;
			this.name = name;
			this.createdAt = createdAt;
		}

		public static Band of(Integer id, String name, Date createdAt) {
			return new Band(id, name, createdAt);
		}

		public static Band convert(ResultSet rs) throws SQLException {
			return new Band(rs.getInt(1), rs.getString(2), rs.getDate(3));
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Date getCreatedAt() {
			return createdAt;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			Band band = (Band) o;
			return Objects.equals(id, band.id) && Objects.equals(name, band.name) && Objects.equals(createdAt, band.createdAt);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name, createdAt);
		}

		@Override
		public String toString() {
			return "Band{" + "id=" + id + ", name='" + name + '\'' + ", createdAt=" + createdAt + '}';
		}
	}
}
