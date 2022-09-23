package fr.maif.akka.jdbc.stream.source;

import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import fr.maif.akka.jdbc.function.ResultSetExtractor;
import fr.maif.akka.jdbc.query.Query;
import fr.maif.akka.jdbc.query.SqlContext;
import fr.maif.akka.jdbc.stream.ResourcesHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by adelegue on 30/04/2016.
 */
public class SelectQuerySource<T> extends GraphStage<SourceShape<T>> implements ResourcesHelper {

	private final static Logger LOGGER = LoggerFactory.getLogger(SelectQuerySource.class);

	private final SqlContext sqlContext;

	private final Query query;

	private final ResultSetExtractor<T> rsConverter;

	private final Boolean closeConnection;

	// Define the (sole) output port of this stage
	private final Outlet<T> out = Outlet.create("ResultSet.out");

	// Define the shape of this stage, which is SourceShape with the port we defined above
	private final SourceShape<T> shape = SourceShape.of(out);

	public SelectQuerySource(SqlContext sqlContext, Query query, ResultSetExtractor<T> rsConverter, Boolean closeConnection) {
		super();
		this.sqlContext = sqlContext;
		this.query = query;
		this.rsConverter = rsConverter;
		this.closeConnection = closeConnection == null ? Boolean.TRUE : closeConnection;
	}

	@Override
	public SourceShape<T> shape() {
		return shape;
	}

	@Override
	public GraphStageLogic createLogic(Attributes inheritedAttributes) {
		return new GraphStageLogic(shape()) {

			final LoggingAdapter log = Logging.getLogger(sqlContext.actorSystem, this);

			private Boolean executed = false;
			private ResultSet resultSet;
			private Statement statement;

			@Override
			public void postStop() {
				try {
					if (Boolean.TRUE.equals(closeConnection)) {
						LOGGER.debug("[Akka stream JDBC select source] - Closing connexion");
					}
					cleanResources(statement, sqlContext.connection, closeConnection);
				} catch (SQLException e) {
					LOGGER.error("[Akka stream JDBC select source] - Error closing connexion", e);
				}
			}

			{
				setHandler(out, new AbstractOutHandler() {

					@Override
					public void onPull() throws Exception {
						try {
							if (!executed) {
								log.debug("[Akka stream JDBC select source] - Preparing statement for query {} ", query);
								PreparedStatement preparedStatement = query.buildPreparedStatement(sqlContext.connection);
								statement = preparedStatement;
								log.debug("[Akka stream JDBC select source] - Executing query {} ", query);
								resultSet = preparedStatement.executeQuery();
								executed = true;
							}
							boolean next = resultSet.next();
							if (next) {
								push(out, rsConverter.get(resultSet));
							} else {
								complete(out);
							}
						} catch (SQLException e) {
							LOGGER.error("[Akka stream JDBC select source] - Error executing request {} with params {}", query.sql(), query.params());
							cleanResources(statement, sqlContext.connection, Boolean.TRUE);
							fail(out, e);
						}
					}
				});
			}
		};
	}
}
