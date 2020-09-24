package fr.maif.jdbc;

import akka.japi.Pair;
import akka.japi.tuple.Tuple3;
import akka.japi.tuple.Tuple4;
import fr.maif.jdbc.function.ResultSetExtractor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by adelegue on 06/05/2016.
 */
public class Convertions {

	public static ResultSetExtractor<Map<String, String>> map = resultSet -> {
		Map<String, String> result = new HashMap<>();
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			String name = metaData.getColumnName(i);
			result.put(name, resultSet.getString(name));
		}
		return result;
	};

	public static Map<String, String> map(ResultSet resultSet) throws SQLException {
		return map.get(resultSet);
	}

	public static <A> ResultSetExtractor<A> as(Class<A> classA) {
		return resultSet -> classA.cast(getAs(resultSet.getObject(1), classA));
	}

	public static <A, B> ResultSetExtractor<Pair<A, B>> as(Class<A> classA, Class<B> classB) {
		return resultSet -> Pair.apply(classA.cast(getAs(resultSet.getObject(1), classA)), classB.cast(getAs(resultSet.getObject(2), classB)));
	}

	public static <A, B, C> ResultSetExtractor<Tuple3<A, B, C>> as(Class<A> classA, Class<B> classB, Class<C> classC) {
		return resultSet -> Tuple3.apply(classA.cast(getAs(resultSet.getObject(1), classA)), classB.cast(getAs(resultSet.getObject(2), classB)),
				classC.cast(getAs(resultSet.getObject(3), classC)));
	}

	public static <A, B, C, D> ResultSetExtractor<Tuple4<A, B, C, D>> as(Class<A> classA, Class<B> classB, Class<C> classC, Class<D> classD) {
		return resultSet -> Tuple4.apply(classA.cast(getAs(resultSet.getObject(1), classA)), classB.cast(getAs(resultSet.getObject(2), classB)),
				classC.cast(getAs(resultSet.getObject(3), classC)), classD.cast(getAs(resultSet.getObject(4), classD)));
	}

	public static <A> Object getAs(Object o, Class<A> cls) {
		if (o == null)
			return o;
		else if (cls.isAssignableFrom(o.getClass())) {
			return o;
		} else {
			if (cls.isAssignableFrom(String.class)) {
				return o.toString();
			} else if (o instanceof Date) {
				Date d = (Date) o;
				if (cls.isAssignableFrom(Long.class))
					return d.getTime();
				else if (cls.isAssignableFrom(BigInteger.class))
					return BigInteger.valueOf(d.getTime());
				else
					return o;
			} else if (o instanceof Timestamp) {
				Timestamp t = (Timestamp) o;
				if (cls.isAssignableFrom(Long.class))
					return t.getTime();
				else if (cls.isAssignableFrom(BigInteger.class))
					return BigInteger.valueOf(t.getTime());
				else
					return o;
			} else if (o instanceof Time) {
				Time t = (Time) o;
				if (cls.isAssignableFrom(Long.class))
					return t.getTime();
				else if (cls.isAssignableFrom(BigInteger.class))
					return BigInteger.valueOf(t.getTime());
				else
					return o;
			} else if (o instanceof BigInteger && cls.isAssignableFrom(Long.class)) {
				return ((BigInteger) o).longValue();
			} else if (o instanceof BigInteger && cls.isAssignableFrom(Integer.class)) {
				return ((BigInteger) o).intValue();
			} else if (o instanceof BigInteger && cls.isAssignableFrom(Double.class)) {
				return ((BigInteger) o).doubleValue();
			} else if (o instanceof BigInteger && cls.isAssignableFrom(Float.class)) {
				return ((BigInteger) o).floatValue();
			} else if (o instanceof BigInteger && cls.isAssignableFrom(Short.class)) {
				return ((BigInteger) o).shortValue();
			} else if (o instanceof BigInteger && cls.isAssignableFrom(BigDecimal.class)) {
				return new BigDecimal((BigInteger) o);
			} else if (o instanceof BigDecimal && cls.isAssignableFrom(Double.class)) {
				return ((BigDecimal) o).doubleValue();
			} else if (o instanceof BigDecimal && cls.isAssignableFrom(Integer.class)) {
				return ((BigDecimal) o).toBigInteger().intValue();
			} else if (o instanceof BigDecimal && cls.isAssignableFrom(Float.class)) {
				return ((BigDecimal) o).floatValue();
			} else if (o instanceof BigDecimal && cls.isAssignableFrom(Short.class)) {
				return ((BigDecimal) o).toBigInteger().shortValue();
			} else if (o instanceof BigDecimal && cls.isAssignableFrom(Long.class)) {
				return ((BigDecimal) o).toBigInteger().longValue();
			} else if (o instanceof BigDecimal && cls.isAssignableFrom(BigInteger.class)) {
				return ((BigDecimal) o).toBigInteger();
			} else if ((o instanceof Short || o instanceof Integer || o instanceof Long) && cls.isAssignableFrom(BigInteger.class)) {
				return new BigInteger(o.toString());
			} else if (o instanceof Number && cls.isAssignableFrom(BigDecimal.class)) {
				return new BigDecimal(o.toString());
			} else if (o instanceof Number && cls.isAssignableFrom(Short.class))
				return ((Number) o).shortValue();
			else if (o instanceof Number && cls.isAssignableFrom(Integer.class))
				return ((Number) o).intValue();
			else if (o instanceof Number && cls.isAssignableFrom(Integer.class))
				return ((Number) o).intValue();
			else if (o instanceof Number && cls.isAssignableFrom(Long.class))
				return ((Number) o).longValue();
			else if (o instanceof Number && cls.isAssignableFrom(Float.class))
				return ((Number) o).floatValue();
			else if (o instanceof Number && cls.isAssignableFrom(Double.class))
				return ((Number) o).doubleValue();
			else
				return o;
		}
	}

}
