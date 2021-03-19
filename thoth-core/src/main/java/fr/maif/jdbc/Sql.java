package fr.maif.jdbc;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.javadsl.Source;
import fr.maif.jdbc.function.ResultSetExtractor;
import fr.maif.jdbc.query.SelectQueryBuilder;
import fr.maif.jdbc.query.SqlContext;
import fr.maif.jdbc.query.UpdateQueryBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Created by adelegue on 29/04/2016.
 */
public class Sql {

	private final Connection connection;
	private final ActorSystem actorSystem;

	Sql(Connection connection, ActorSystem actorSystem) {
		this.connection = connection;
		this.actorSystem = actorSystem;
	}

	public static Sql of(Connection connection, ActorSystem actorSystem) {
		return new Sql(connection, actorSystem);
	}

	public SelectQueryBuilder<ResultSet> select(String query) {
		return new SelectQueryBuilder<>(query, new SqlContext(actorSystem, connection), ResultSetExtractor.identity());
	}

	public UpdateQueryBuilder update(String query) {
		return new UpdateQueryBuilder(query, new SqlContext(actorSystem, connection));
	}

	public static Source<Connection, NotUsed> connection(DataSource dataSource, ActorSystem actorSystem) {
		return connection(dataSource, actorSystem.dispatchers().lookup("jdbc-execution-context"), true);
	}

	public static Source<Connection, NotUsed> connection(DataSource dataSource, Executor executor) {
		return connection(dataSource, executor, true);
	}

	public static Source<Connection, NotUsed> connection(DataSource dataSource, Executor executor, Boolean autocommit) {
		return Source.completionStage(connectionCS(dataSource, executor, autocommit));
	}

	public static CompletionStage<Connection> connectionCS(DataSource dataSource, Executor executor, Boolean autocommit) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				Connection connection = dataSource.getConnection();
				try {
					if (Boolean.FALSE.equals(autocommit)) {
						connection.setAutoCommit(false);
					}
					return connection;
				} catch (SQLException e) {
					connection.close();
					throw new RuntimeException(e);
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}, executor);
	}

	public static CompletionStage<Done> close(Connection connection, JdbcExecutionContext executor) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				connection.close();
				return Done.getInstance();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}, executor);
	}

	public static CompletionStage<Done> commitAndClose(Connection connection, JdbcExecutionContext executor) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				connection.commit();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} finally {
				try {
					connection.close();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			return Done.getInstance();
		}, executor);
	}


	public static CompletionStage<Done> rollbackAndClose(Connection connection, JdbcExecutionContext executor) {
		return CompletableFuture.supplyAsync(() -> {

			try {
				connection.rollback();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			} finally {
				try {
					connection.close();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}

			return Done.getInstance();
		}, executor);
	}

	public ActorSystem getActorSystem() {
		return actorSystem;
	}
}
