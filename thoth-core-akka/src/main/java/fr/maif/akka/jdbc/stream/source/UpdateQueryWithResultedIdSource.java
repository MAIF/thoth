package fr.maif.akka.jdbc.stream.source;

import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import fr.maif.akka.jdbc.query.Query;
import fr.maif.akka.jdbc.query.SqlContext;
import fr.maif.akka.jdbc.stream.ResourcesHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by adelegue on 12/10/2016.
 */
public class UpdateQueryWithResultedIdSource<T> extends GraphStage<SourceShape<List<T>>> implements ResourcesHelper {

	private static final Logger LOGGER = LoggerFactory.getLogger(UpdateQueryWithResultedIdSource.class);

	private final SqlContext sqlContext;

	private final Query query;

	private final Boolean closeConnection;

	// Define the (sole) output port of this stage
	private final Outlet<List<T>> out = Outlet.create("ResultSet.out");

	// Define the shape of this stage, which is SourceShape with the port we defined above
	private final SourceShape<List<T>> shape = SourceShape.of(out);

	public UpdateQueryWithResultedIdSource(SqlContext sqlContext, Query query, Boolean closeConnection) {
		super();
		this.sqlContext = sqlContext;
		this.query = query;
		this.closeConnection = closeConnection;
	}

	@Override
	public SourceShape<List<T>> shape() {
		return shape;
	}

    @SuppressWarnings("unchecked")
	@Override
	public GraphStageLogic createLogic(Attributes inheritedAttributes) {
		return new GraphStageLogic(shape()) {

			final LoggingAdapter log = Logging.getLogger(sqlContext.actorSystem, this);

			private Boolean executed = false;
			private int result;
			private Statement statement;

			@Override
			public void postStop() {
				try {
					if (Boolean.TRUE.equals(closeConnection)) {
						LOGGER.debug("[Akka stream JDBC update source] - Closing connexion");
					}
					cleanResources(statement, sqlContext.connection, closeConnection);
				} catch (SQLException e) {
					LOGGER.error("[Akka stream JDBC update source] - Error closing connexion", e);
				}
			}

			{
				setHandler(out, new AbstractOutHandler() {

					@Override
					public void onPull() throws Exception {
						if (!executed) {
							try {
								log.debug("[Akka stream JDBC update source] - Preparing statement for update query {}", query);
								PreparedStatement preparedStatement = query.buildPreparedStatement(sqlContext.connection);
								statement = preparedStatement;
								log.debug("[Akka stream JDBC update source] - Executing update query {}", query);
								ResultSet generatedKeys = preparedStatement.executeQuery();
								executed = true;
								List<T> keys = new ArrayList<>();
								//ResultSet generatedKeys = statement.getGeneratedKeys();
								while (generatedKeys.next()) {
									keys.add((T)generatedKeys.getObject(1));
								}
								push(out, keys);
								complete(out);
							} catch (SQLException e) {
								LOGGER.error("[Akka stream JDBC update source] - Error executing request {} with params {}", query.sql(),
										query.params());
								cleanResources(statement, sqlContext.connection, Boolean.TRUE);
								fail(out, e);
							}
						} else {
							complete(out);
						}
					}
				});
			}
		};
	}
}