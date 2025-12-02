package imitator.threads;

import java.sql.*;


public class NullThread extends BaseImitationThread {
    public NullThread(Connection connection,  CurrentData currentData) {
        super(connection, currentData, "NullThread");

    }

    @Override
    public void run() {
        System.err.println("Ошибка при определении типа пользователя");
    }

}
