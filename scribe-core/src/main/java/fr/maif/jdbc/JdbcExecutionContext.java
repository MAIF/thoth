package fr.maif.jdbc;

import akka.actor.ActorSystem;
import fr.maif.akka.AkkaExecutionContext;

public class JdbcExecutionContext extends AkkaExecutionContext {

    public JdbcExecutionContext(ActorSystem system) {
        this(system, "jdbc-execution-context");
    }

    public JdbcExecutionContext(ActorSystem system, String name) {
        super(system, name);
    }
}
