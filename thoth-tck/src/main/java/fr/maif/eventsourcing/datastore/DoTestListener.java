package fr.maif.eventsourcing.datastore;

import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

public class DoTestListener extends TestListenerAdapter {
    @Override
    public void onTestFailure(ITestResult tr) {
        System.out.println(tr.getName() + " FAILED");
    }

    @Override
    public void onTestSkipped(ITestResult tr) {
        System.out.println(tr.getName() + " SKIPPED");
    }

    @Override
    public void onTestSuccess(ITestResult tr) {
        System.out.println(tr.getName() + " SUCCESSFULL");
    }
}
