package com.example.demo;

import com.fasterxml.jackson.databind.JsonNode;
import fr.maif.eventsourcing.Result;
import fr.maif.eventsourcing.vanilla.format.JacksonEventFormat;
import fr.maif.json.Json;
import fr.maif.json.JsonWrite;

import static com.example.demo.BankEvent.*;

public class BankEventFormat implements JacksonEventFormat<String, BankEvent> {

    @Override
    public Result<String, BankEvent> read(String type, Long version, JsonNode json) {

        record Tuple(String type, Long version) {};

        return switch (new Tuple(type, version)) {
            case Tuple t when MoneyDepositedV1.match(t.type, t.version) ->
                    Result.fromEither(Json.fromJson(json, BankEvent.MoneyDeposited.class).toEither().mapLeft(errs -> errs.mkString(",")));
            case Tuple t when MoneyWithdrawnV1.match(t.type, t.version) ->
                    Result.fromEither(Json.fromJson(json, BankEvent.MoneyWithdrawn.class).toEither().mapLeft(errs -> errs.mkString(",")));
            case Tuple t when AccountClosedV1.match(t.type, t.version) ->
                    Result.fromEither(Json.fromJson(json, BankEvent.AccountClosed.class).toEither().mapLeft(errs -> errs.mkString(",")));
            case Tuple t when AccountOpenedV1.match(t.type, t.version) ->
                    Result.fromEither(Json.fromJson(json, BankEvent.AccountOpened.class).toEither().mapLeft(errs -> errs.mkString(",")));
            default -> Result.<String, BankEvent>error("Unknown event type " + type + "(v" + version + ")");
        };
    }

    @Override
    public JsonNode write(BankEvent event) {
        return Json.toJson(event, JsonWrite.auto());
    }
}
