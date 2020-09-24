package fr.maif.jdbc.function;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by adelegue on 01/05/2016.
 */
@FunctionalInterface
public interface ResultSetExtractor<T> {

	T get(ResultSet resultSet) throws SQLException;

	static ResultSetExtractor<ResultSet> identity() {
		return resultSet -> resultSet;
	}

}
