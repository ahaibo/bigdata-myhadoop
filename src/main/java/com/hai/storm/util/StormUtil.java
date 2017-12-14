package com.hai.storm.util;

import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by as on 2017/4/11.
 */
public class StormUtil {

    public static void log2nc(Object o, String msg) {
        try {
            Runtime runtime = Runtime.getRuntime();
            Process process = runtime.exec("nc 192.168.0.103 19888");
            OutputStream out = process.getOutputStream();

            Date date = new Date();
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String time = dateFormat.format(date);

            String host = InetAddress.getLocalHost().getHostName();

            //pid
            RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
            //8088@s1
            String pid = runtimeMXBean.getName().split("@")[0];

            String threadInfo = "T@" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId();
            String objInfo = o.getClass().getName() + "@" + o.hashCode();

            StringBuilder prefix = new StringBuilder();
            prefix
                    .append("[")
                    .append(time).append(" ")
                    .append(host).append(" ")
                    .append("PID-").append(pid).append(" ")
                    .append(threadInfo).append(" ")
                    .append(objInfo).append(" ")
                    .append("]")
                    .append(msg);

            String info = prefix.toString();
            System.out.println(info);
            //write to nc server
            out.write(info.getBytes());
            out.flush();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] int2bytes(int i) {
        byte[] arr = new byte[4];
        arr[0] = (byte) (i >>> 24);
        arr[1] = (byte) (i >>> 16);
        arr[2] = (byte) (i >>> 8);
        arr[3] = (byte) (i >>> 0);
        return arr;
    }

    public static int bytes2int(byte[] bytes) {
        return bytes[0] << 24 | bytes[1] << 16 | bytes[2] << 8 | bytes[3] << 0;
    }
}
