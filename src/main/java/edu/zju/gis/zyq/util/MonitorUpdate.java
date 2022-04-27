package edu.zju.gis.zyq.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Zhou Yuquan
 * @version 1.0, 2021-12-09
 */
public class MonitorUpdate {

    public static void update(long id, String addr, String database, String username, String password) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String t = sdf.format(new Date());
        PGHelper pgHelper = new PGHelper("jdbc:postgresql://" + addr + "/" + database, username, password);
        pgHelper.run(String.format("update public.monitor set status = 'Success', \"end\" = '%s' where id = %d", t, id));
    }

}
