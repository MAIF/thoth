package fr.maif.akka.jdbc.exceptions;

import akka.japi.function.Creator;
import akka.japi.function.Effect;

/**
 * Created by adelegue on 29/04/2016.
 */
public final class ExceptionsHandler {

	private ExceptionsHandler() {}

	public static <T> T handleChecked(Creator<T> toExecute) {
		try {
			return toExecute.create();
		} catch (Exception e) {
			throw new SqlException(e);
		}
	}

	public static void handleChecked0(Effect toExecute) {
		try {
			toExecute.apply();
		} catch (Exception e) {
			throw new SqlException(e);
		}
	}

}
