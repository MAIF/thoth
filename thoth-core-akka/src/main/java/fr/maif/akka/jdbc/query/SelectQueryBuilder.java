package fr.maif.akka.jdbc.query;

import akka.NotUsed;
import akka.stream.ActorAttributes;
import akka.stream.Attributes;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import fr.maif.akka.jdbc.function.ResultSetExtractor;
import fr.maif.akka.jdbc.stream.source.SelectQuerySource;

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
public class SelectQueryBuilder<Out> implements Query {

	final String sql;

	final SqlContext sqlContext;

	final Integer resultSetType;

	final Integer resultSetConcurrency;

	final Integer resultSetHoldability;

	final List<Object> params;

	final Boolean closeConnection;

	final Integer fetchSize;

	final ResultSetExtractor<Out> resultSetExtractor;

	public SelectQueryBuilder(String sql, SqlContext sqlContext, ResultSetExtractor<Out> resultSetExtractor) {
		this(sql, sqlContext, null, null, null, null, Boolean.TRUE, 1000, resultSetExtractor);
	}

	public SelectQueryBuilder(String sql, SqlContext sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability,
							  List<Object> params, Boolean closeConnection, Integer fetchSize, ResultSetExtractor<Out> resultSetExtractor) {
		super();
		this.sql = sql;
		this.sqlContext = sqlContext;
		this.resultSetType = resultSetType;
		this.resultSetConcurrency = resultSetConcurrency;
		this.resultSetHoldability = resultSetHoldability;
		this.params = params;
		this.closeConnection = closeConnection;
		this.fetchSize = fetchSize;
		this.resultSetExtractor = resultSetExtractor;
	}

	@Override
	public String toString() {
		return "SelectQueryBuilder{" +
				"sql='" + sql + '\'' +
				", params=" + params +
				'}';
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
	public Integer fetchSize() {
		return fetchSize;
	}

	@Override
	public List<Object> params() {
		return this.params;
	}

	public SelectQueryBuilder<Out> withFetchSize(Integer fetchSize) {
		return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection,
				fetchSize, resultSetExtractor);
	}

	public SelectQueryBuilder<Out> withResultSetType(Integer resultSetType) {
		return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection,
				fetchSize, resultSetExtractor);
	}

	public SelectQueryBuilder<Out> withResultSetConcurrency(Integer resultSetConcurrency) {
		return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection,
				fetchSize, resultSetExtractor);
	}

	public SelectQueryBuilder<Out> paramsList(List<Object> params) {
		return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection,
				fetchSize, resultSetExtractor);
	}

	public SelectQueryBuilder<Out> params(Object... params) {
		return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, Arrays.asList(params),
				closeConnection, fetchSize, resultSetExtractor);
	}

	public SelectQueryBuilder<Out> param(Object param) {
		if (params == null) {
			return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability,
					unmodifiableList(singletonList(param)), closeConnection, fetchSize, resultSetExtractor);
		} else {
			return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability,
					unmodifiableList(Stream.concat(params.stream(), Stream.of(param)).collect(Collectors.toList())), closeConnection,
					fetchSize, resultSetExtractor);
		}
	}

	public SelectQueryBuilder<Out> closeConnection(Boolean closeConnection) {
		return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection,
				fetchSize, resultSetExtractor);
	}

	protected <Mat> Source<Out, Mat> applyDispatcher(Source<Out, Mat> aSource, Optional<String> dispatcher) {
		return dispatcher.map(d -> aSource.withAttributes(ActorAttributes.dispatcher(d))).orElse(aSource);
	}

	public <In, Mat> Flow<In, Out, Mat> applyDispatcher(Flow<In, Out, Mat> aFlow, Optional<String> dispatcher) {
		return dispatcher.map(d -> aFlow.withAttributes(ActorAttributes.dispatcher(d))).orElse(aFlow);
	}

	public <Out2> SelectQueryBuilder<Out2> as(ResultSetExtractor<Out2> resultSetExtractor) {
		return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, closeConnection,
				fetchSize, resultSetExtractor);
	}

	public <Out2> Source<Out2, NotUsed> get(ResultSetExtractor<Out2> rsExtract) {
		return as(rsExtract).get();
	}

	public Source<Out, NotUsed> get() {
		return Source.fromGraph(new SelectQuerySource<Out>(sqlContext, this, resultSetExtractor, closeConnection))
                //FIXME use .withAttributes(ActorAttributes.dispatcher("akka.actor.jdbc-execution-context"))
				.withAttributes(Attributes.apply(ActorAttributes.IODispatcher()));
	}
}
