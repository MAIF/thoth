package fr.maif.thoth.sample.api;

import java.math.BigDecimal;
import java.util.Objects;

import fr.maif.eventsourcing.ProcessingSuccess;
import fr.maif.thoth.sample.events.BankEvent;
import fr.maif.thoth.sample.state.Account;
import io.vavr.Tuple0;
import io.vavr.control.Either;

public class AccountDTO {
    public BigDecimal balance;
    public String id;
    public String error;

    public AccountDTO() {
    }

    public AccountDTO(BigDecimal balance, String id) {
        this.balance = balance;
        this.id = id;
    }

    private AccountDTO(String error) {
        this.error = error;
    }

    public static AccountDTO fromAccount(Account account) {
        return new AccountDTO(account.balance, account.id);
    }

    public static AccountDTO error(String err) {
        return new AccountDTO(err);
    }

    public static AccountDTO create(
            Either<
                    String,
                    ProcessingSuccess<
                            Account,
                            BankEvent,
                            Tuple0, Tuple0, Tuple0
                    >
            > maybeSuccess) {
        return maybeSuccess.fold(
                AccountDTO::new,
                success -> success.currentState.map(AccountDTO::fromAccount).get()
        );

    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AccountDTO that = (AccountDTO) o;
        return Objects.equals(balance, that.balance) && Objects.equals(id, that.id) && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(balance, id, error);
    }

    @Override
    public String toString() {
        return "AccountDTO{" +
                "balance=" + balance +
                ", id='" + id + '\'' +
                ", error='" + error + '\'' +
                '}';
    }
}
