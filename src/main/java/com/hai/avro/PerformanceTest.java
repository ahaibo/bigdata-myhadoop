package com.hai.avro;

import com.hai.avro.source.MyUser;
import com.hai.avro.source.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;

/**
 * Created by as on 2017/3/26.
 */
public class PerformanceTest {

    private static final int MAX_COUNT = 1000000;

    public static void main(String[] args) {
//        serial();
        deSerial();
    }

    public static void serial() {
        try {
            clean();
            System.out.println("\nserial start...");
            javaSerial();
            writeableSerial();
            avroSerial();
            System.out.println("\nserial end...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deSerial() {
        try {
            System.out.println("\ndeSerial start...");
            javaDeSerial();
            writeableDeSerial();
            avroDeSerial();
            System.out.println("\ndeSerial end...");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void clean() {
        File file = new File("file/avro");
        if (null != file && file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files) {
                if (f.canExecute()) {
                    f.delete();
                    System.out.println("deleted file: " + f.getName());
                }
            }
        }
    }

    public static void javaSerial() throws IOException {

        long time = System.currentTimeMillis();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileOutputStream fos = new FileOutputStream("file/avro/users.java.serial");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        MyUser user = null;

        for (int i = 0; i < MAX_COUNT; i++) {
            user = new MyUser();
            user.setName("tom" + i);
            user.setFavoriteNumber(i);
            user.setFavoriteColor("red" + i);
            oos.writeObject(user);
        }
        oos.close();

        System.out.println("javaSerial time: " + (System.currentTimeMillis() - time) + "\tsize: " + baos.toByteArray().length);
    }

    public static void javaDeSerial() throws IOException, ClassNotFoundException {

        long time = System.currentTimeMillis();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileInputStream fis = new FileInputStream("file/avro/users.java.serial");
        ObjectInputStream oos = new ObjectInputStream(fis);
        MyUser user = null;

        for (int i = 0; i < MAX_COUNT; i++) {
            user = (MyUser) oos.readObject();
        }
        oos.close();

        System.out.println("javaDeSerial time: " + (System.currentTimeMillis() - time) + "\tsize: " + baos.toByteArray().length);
    }

    public static void writeableSerial() throws IOException {

        long time = System.currentTimeMillis();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileOutputStream fos = new FileOutputStream("file/avro/users.writeable.serial");
        DataOutputStream dos = new DataOutputStream(fos);
        MyUser user = null;
        for (int i = 0; i < MAX_COUNT; i++) {
//            dos.writeUTF("tom" + i);
//            dos.writeInt(i);
//            dos.writeUTF("red" + i);
            user = new MyUser("tom" + i, i, "red" + i);
            user.write(dos);
        }
        dos.close();

        System.out.println("writeableSerial time: " + (System.currentTimeMillis() - time) + "\tsize: " + baos.toByteArray().length);
    }

    public static void writeableDeSerial() throws IOException {

        long time = System.currentTimeMillis();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileInputStream fis = new FileInputStream("file/avro/users.writeable.serial");
        DataInputStream dos = new DataInputStream(fis);
        MyUser user = new MyUser();
        for (int i = 0; i < MAX_COUNT; i++) {
            user.readFields(dos);
        }
        dos.close();

        System.out.println("writeableDeSerial time: " + (System.currentTimeMillis() - time) + "\tsize: " + baos.toByteArray().length);
    }

    public static void avroSerial() throws IOException {

        long time = System.currentTimeMillis();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        FileOutputStream fos = new FileOutputStream("file/avro/users.avro.serial");

        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> userDataFileWriter = new DataFileWriter<User>(userDatumWriter);

        User user = new User();
        userDataFileWriter.create(user.getSchema(), fos);//avro数据文件

        for (int i = 0; i < MAX_COUNT; i++) {
            User u = new User();
            u.setName("tom" + i);
            u.setFavoriteNumber(i);
            u.setFavoriteColor("red" + i);
            userDataFileWriter.append(u);
        }
        userDataFileWriter.close();

        System.out.println("avroSerial time: " + (System.currentTimeMillis() - time) + "\tsize: " + baos.toByteArray().length);
    }

    public static void avroDeSerial() throws IOException {

        long time = System.currentTimeMillis();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        File file = new File("file/avro/users.avro.serial");

        DatumReader<User> userDatumWriter = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> userDataFileWriter = new DataFileReader<User>(file, userDatumWriter);

        User user = null;
        while (userDataFileWriter.hasNext()) {
            user = userDataFileWriter.next();
        }
        userDataFileWriter.close();

        System.out.println("avroDeSerial time: " + (System.currentTimeMillis() - time) + "\tsize: " + baos.toByteArray().length);
    }

}
