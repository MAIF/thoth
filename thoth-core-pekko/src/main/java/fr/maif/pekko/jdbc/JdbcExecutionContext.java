package fr.maif.pekko.jdbc;

import org.apache.pekko.actor.ActorSystem;
import fr.maif.pekko.PekkoExecutionContext;

public class JdbcExecutionContext extends PekkoExecutionContext {

    public JdbcExecutionContext(ActorSystem system) {
        this(system, "jdbc-execution-context");
    }

    public JdbcExecutionContext(ActorSystem system, String name) {
        super(system, name);
    }
}
