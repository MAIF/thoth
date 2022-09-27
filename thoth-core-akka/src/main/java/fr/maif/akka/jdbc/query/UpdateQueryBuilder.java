package fr.maif.akka.jdbc.query;

import akka.NotUsed;
import akka.stream.ActorAttributes;
import akka.stream.Attributes;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import fr.maif.akka.jdbc.stream.source.UpdateQuerySource;
import fr.maif.akka.jdbc.stream.source.UpdateQueryWithResultedIdSource;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

/**
 *
 * Created by adelegue on 29/04/2016.
 */
public class UpdateQueryBuilder implements Query {

	final String sql;

	final SqlContext sqlContext;

	final Integer resultSetType;

	final Integer resultSetConcurrency;

	final Integer resultSetHoldability;

	final List<Object> params;

	final Boolean closeConnection;

	public UpdateQueryBuilder(String sql, SqlContext sqlContext) {
		this(sql, sqlContext, null, null, null, null, Boolean.TRUE);
	}

	public UpdateQueryBuilder(String sql, SqlContext sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability,
			List<Object> params, Boolean closeConnection) {
		super();
		this.sql = sql;
		this.sqlContext = sqlContext;
		this.resultSetType = resultSetType;
		this.resultSetConcurrency = resultSetConcurrency;
		this.resultSetHoldability = resultSetHoldability;
		this.params = params;
		this.closeConnection = closeConnection;
	}

	@Override
	public String sql() {
		return this.sql;
	}

	@Override
	public Integer resultSetType() {
		return this.resultSetType;
	}

	@Override
	public Integer resultSetConcurrency() {
		return this.resultSetConcurrency;
	}

	@Override
	public Integer resultSetHoldability() {
		return this.resultSetHoldability;
	}

	@Override
	public List<Object> params() {
		return this.params;
	}

	public UpdateQueryBuilder withResultSetType(Integer resultSetType) {
		return new UpdateQueryBuilder(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection);
	}

	public UpdateQueryBuilder withResultSetConcurrency(Integer resultSetConcurrency) {
		return new UpdateQueryBuilder(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection);
	}

	public UpdateQueryBuilder paramsList(List<Object> params) {
		return new UpdateQueryBuilder(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection);
	}

	public UpdateQueryBuilder params(Object... params) {
		return new UpdateQueryBuilder(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, Arrays.asList(params),
				closeConnection);
	}

	public UpdateQueryBuilder param(Object param) {
		if (params == null) {
			return new UpdateQueryBuilder(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability,
					unmodifiableList(singletonList(param)), closeConnection);
		} else {
			return new UpdateQueryBuilder(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability,
					unmodifiableList(Stream.concat(params.stream(), Stream.of(param)).collect(Collectors.toList())), closeConnection);
		}
	}

	public UpdateQueryBuilder closeConnection(Boolean closeConnection) {
		return new UpdateQueryBuilder(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection);
	}

	protected <Mat> Source<Integer, Mat> applyDispatcher(Source<Integer, Mat> aSource, Optional<String> dispatcher) {
		return dispatcher.map(d -> aSource.withAttributes(ActorAttributes.dispatcher(d))).orElse(aSource);
	}

	public <In, Mat> Flow<In, Integer, Mat> applyDispatcher(Flow<In, Integer, Mat> aFlow, Optional<String> dispatcher) {
		return dispatcher.map(d -> aFlow.withAttributes(ActorAttributes.dispatcher(d))).orElse(aFlow);
	}

	public Source<Integer, NotUsed> count() {
		return Source.fromGraph(new UpdateQuerySource(sqlContext, this, closeConnection))
				//FIXME use .withAttributes(ActorAttributes.dispatcher("akka.actor.jdbc-execution-context"))
				.withAttributes(Attributes.apply(ActorAttributes.IODispatcher()));
	}

	public <T> Source<List<T>, NotUsed> generatedKey() {
		return Source.fromGraph(new UpdateQueryWithResultedIdSource<T>(sqlContext, this, closeConnection))
				//FIXME use .withAttributes(ActorAttributes.dispatcher("akka.actor.jdbc-execution-context"))
				.withAttributes(Attributes.apply(ActorAttributes.IODispatcher()));
	}
}
