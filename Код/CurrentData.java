package imitator.threads;

import imitator.ImitatorConfig;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Random;

public class CurrentData {
    private Timestamp currentData;
    protected final Random random;

    public CurrentData(Date currentData){
        this.currentData = new Timestamp (currentData.getTime());;
        this.random = new Random();
    }

    public Timestamp  date_run(int interval1min) {
        long startMillis = currentData.getTime();
        long randomMillis = startMillis + (long) (random.nextDouble() * (interval1min * 60000));
        Timestamp gen = new Timestamp(randomMillis);

        if (gen.compareTo(ImitatorConfig.END_DATE) <= 0) {
            return gen;
        } else {
            return new Timestamp(ImitatorConfig.END_DATE.getTime());
        }
    }

    public void run(){
        long startMillis = currentData.getTime();
        currentData = new Timestamp(startMillis + (long) (random.nextDouble() * (60000)));
    }

    @Override
    public String  toString() {
        return currentData.toString();
    }

}
