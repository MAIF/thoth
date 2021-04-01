package fr.maif.thoth.sample.projections.eventualyconsistent;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import akka.actor.ActorSystem;
import fr.maif.projections.EventuallyConsistentProjection;
import fr.maif.thoth.sample.events.BankEvent;
import fr.maif.thoth.sample.events.BankEventFormat;
import io.vavr.Tuple;
import io.vavr.concurrent.Future;

@Component
public class MeanWithdrawProjection  {
    private static final Logger LOGGER = LoggerFactory.getLogger(MeanWithdrawProjection.class);

    private final ActorSystem actorSystem;
    private final String bootstrapServer;
    private final BankEventFormat eventFormat;
    private final DataSource dataSource;

    public MeanWithdrawProjection(
            ActorSystem actorSystem,
            @Value("${kafka.port}") int port,
            @Value("${kafka.host}") String host,
            BankEventFormat eventFormat,
            DataSource dataSource) {
        this.actorSystem = actorSystem;
        this.eventFormat = eventFormat;
        this.dataSource = dataSource;
        this.bootstrapServer = host + ":" + port;
    }

    @PostConstruct
    public void init() {
        EventuallyConsistentProjection.create(
                actorSystem,
                "MeanWithdrawProjection",
                EventuallyConsistentProjection.Config.create("bank", "MeanWithdrawProjection", bootstrapServer),
                eventFormat,
                envelope -> {
                    return Future.of(() -> {
                        if(envelope.event instanceof BankEvent.MoneyWithdrawn withdraw) {
                            try(final PreparedStatement statement = dataSource.getConnection().prepareStatement("""
                                insert into withdraw_by_month (client_id, month, year, withdraw, count) values (?, ?, ?, ?, 1)
                                    on conflict on constraint WITHDRAW_BY_MONTH_UNIQUE
                                    do update set withdraw = withdraw_by_month.withdraw + EXCLUDED.withdraw, count=withdraw_by_month.count + 1
                            """)
                            ) {
                                statement.setString(1, envelope.entityId);
                                statement.setString(2, envelope.emissionDate.getMonth().name().toUpperCase());
                                statement.setInt(3, envelope.emissionDate.getYear());
                                statement.setBigDecimal(4, withdraw.amount);

                                statement.execute();
                            } catch (SQLException ex) {
                                LOGGER.error("Failed to update stats projection", ex);
                            }
                        }
                        return Tuple.empty();
                    });
                }
        ).start();
    }

    public Optional<BigDecimal> meanWithdrawByClientAndMonth(String clientId, Integer year, String month) throws SQLException {
        try(final PreparedStatement statement = dataSource.getConnection().prepareStatement(
                "SELECT round(withdraw / count::decimal, 2) FROM withdraw_by_month WHERE client_id = ? and year = ? and month = ?")) {
            statement.setString(1, clientId);
            statement.setInt(2, year);
            statement.setString(3, month.toUpperCase());
            try(final ResultSet resultSet = statement.executeQuery()) {
                if(!resultSet.isBeforeFirst()) {
                    return Optional.empty();
                }
                resultSet.next();
                return Optional.ofNullable(resultSet.getBigDecimal(1));
            }
        }
    }

    public Optional<BigDecimal> meanWithdrawByClient(String clientId) throws SQLException {
        try(final PreparedStatement statement = dataSource.getConnection().prepareStatement(
                "SELECT round(sum(withdraw) / sum(count)::decimal, 2) FROM withdraw_by_month WHERE client_id = ?")) {
            statement.setString(1, clientId);
            try(final ResultSet resultSet = statement.executeQuery()) {
                if(!resultSet.isBeforeFirst()) {
                    return Optional.empty();
                }
                resultSet.next();
                return Optional.ofNullable(resultSet.getBigDecimal(1));
            }
        }
    }
}
