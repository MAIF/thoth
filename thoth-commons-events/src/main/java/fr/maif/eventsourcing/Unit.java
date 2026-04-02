package fr.maif.eventsourcing;

public class Unit {

    public static Unit unit = new Unit();

    public static Unit unit() {
        return unit;
    }

    private Unit() {
    }
}
