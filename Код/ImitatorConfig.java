package imitator;

import java.sql.Date;

public class ImitatorConfig {
    public static final Date START_DATE = Date.valueOf("2025-11-30");
    public static final Date END_DATE = Date.valueOf("2050-11-30");

    public static final int[] ADMINS_ID = {40212, 40428};
    public static final int[] MODERS_ID = {40213, 40214, 40215, 40216, 40217, 40218, 40219, 40220, 40221, 40222, 40223, 40224, 40225, 40226, 40227, 40228, 40229, 40230, 40231, 40232, 40233, 40234, 40235, 40236, 40237,  40238};

    public static final int THREAD_POOL_SIZE = 100;
    public static final int GENERATION_DURATION_MINUTES = 1;
}