package fr.maif.thoth.sample.events;

import java.math.BigDecimal;

import fr.maif.eventsourcing.EventHandler;
import fr.maif.thoth.sample.state.Account;
import io.vavr.control.Option;

public class BankEventHandler implements EventHandler<Account, BankEvent> {
    @Override
    public Option<Account> applyEvent(
            Option<Account> previousState,
            BankEvent event) {
        if(event instanceof BankEvent.AccountClosed accountClosed) {
            return handleAccountClosed(accountClosed);
        } else if (event instanceof BankEvent.AccountOpened accountOpened) {
            return handleAccountOpened(accountOpened);
        } else if(event instanceof BankEvent.MoneyWithdrawn withdraw) {
            return handleMoneyWithdrawn(previousState, withdraw);
        } else if(event instanceof BankEvent.MoneyDeposited deposit) {
            return handleMoneyDeposited(previousState, deposit);
        } else {
            throw new IllegalArgumentException("Event type unknown " + event.type().name());
        }
    }

    private static Option<Account> handleAccountClosed(BankEvent.AccountClosed close) {
        return Option.none();
    }

    private static Option<Account> handleMoneyWithdrawn(
            Option<Account> previousState,
            BankEvent.MoneyWithdrawn withdraw) {
        return previousState.map(account -> {
            return account.toBuilder()
                    .balance(account.balance.subtract(withdraw.amount))
                    .build();
        });
    }

    private static Option<Account> handleAccountOpened(BankEvent.AccountOpened event) {

        Account account = Account.builder()
                .id(event.entityId())
                .balance(BigDecimal.ZERO)
                .build();

        return Option.some(account);
    }

    private static Option<Account> handleMoneyDeposited(
            Option<Account> previousState,
            BankEvent.MoneyDeposited event) {
        return previousState.map(state -> {
            return state.toBuilder().balance(state.balance.add(event.amount)).build();
        });
    }
}
