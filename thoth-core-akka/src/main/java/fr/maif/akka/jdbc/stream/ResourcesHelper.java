package fr.maif.akka.jdbc.stream;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Mixin to handle jdbc resources.
 *
 * Created by adelegue on 01/05/2016.
 */
public interface ResourcesHelper {

	default void cleanResources(Statement statement, Connection sqlConnection, Boolean closeConnexion) throws SQLException {
		try {
			if (statement != null && !statement.isClosed()) {
				statement.close();
			}
		} finally {
			if (Boolean.TRUE.equals(closeConnexion)) {
				sqlConnection.close();
			}
		}
	}
}
