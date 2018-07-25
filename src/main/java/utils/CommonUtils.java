package utils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by 1 on 2018/7/24.
 */
public class CommonUtils {
    public static Timestamp getSqlTime() {
        //获得系统时间.
        Date date = new Date();

        //将时间格式转换成符合Timestamp要求的格式.
        String nowTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
        //把时间转换
        return Timestamp.valueOf(nowTime);
    }
}
