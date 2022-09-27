package fr.maif.akka.jdbc.query;

import akka.actor.ActorSystem;

import java.sql.Connection;

/**
 * Created by adelegue on 30/04/2016.
 */
public class SqlContext {

	public final ActorSystem actorSystem;

	public final Connection connection;

	public SqlContext(ActorSystem actorSystem, Connection connection) {
		this.actorSystem = actorSystem;
		this.connection = connection;
	}
}
