package util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    private Properties properties;

    public Config() {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find config.properties");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading configuration", e);
        }
    }

    public String getDbUrl() { return properties.getProperty("db.url"); }
    public String getDbUsername() { return properties.getProperty("db.username"); }
    public String getDbPassword() { return properties.getProperty("db.password"); }

}