package fr.maif.jdbc.query;

import fr.maif.jdbc.exceptions.ExceptionsHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Created by adelegue on 30/04/2016.
 */
public interface Query {

	String sql();

	Integer resultSetType();

	Integer resultSetConcurrency();

	Integer resultSetHoldability();

	List<Object> params();

	default Integer fetchSize() {
		return 1000;
	}

	default PreparedStatement buildPreparedStatement(Connection connection) {
		return ExceptionsHandler.handleChecked(() -> {
			PreparedStatement preparedStatement;
			if (resultSetConcurrency() != null && resultSetType() != null && resultSetHoldability() != null) {
				preparedStatement = connection.prepareStatement(sql(), resultSetType(), resultSetConcurrency(), resultSetHoldability());
			} else if (resultSetConcurrency() != null && resultSetType() != null) {
				preparedStatement = connection.prepareStatement(sql(), resultSetType(), resultSetConcurrency());
			} else if (resultSetType() != null) {
				preparedStatement = connection.prepareStatement(sql(), resultSetType());
			} else {
				preparedStatement = connection.prepareStatement(sql());
			}
			if (params() != null && !params().isEmpty()) {
				for (int i = 0; i < params().size(); i++) {
					preparedStatement.setObject(i + 1, params().get(i));
				}
			}
			preparedStatement.setFetchSize(fetchSize());
			return preparedStatement;
		});
	}

}
