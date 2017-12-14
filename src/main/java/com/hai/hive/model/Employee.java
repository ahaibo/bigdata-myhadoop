package com.hai.hive.model;

/**
 * Created by as on 2017/3/29.
 */
public class Employee {
    private int id;
    private String name;
    private double salary;
    private String destination;
    private Partition partition;

    public Employee() {
    }

    public Employee(int id, String name, double salary) {
        this.id = id;
        this.name = name;
        this.salary = salary;
    }

    public Employee(int id, String name, double salary, String destination) {
        this(id, name, salary);
        this.destination = destination;
    }

    public Employee(int id, String name, double salary, String destination, Partition partition) {
        this(id, name, salary, destination);
        this.partition = partition;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", salary=" + salary +
                ", destination='" + destination + '\'' +
                ", partition=" + partition +
                '}';
    }

    public static class Partition {

        public Partition() {
        }

        public Partition(String province, String city) {
            this.province = province;
            this.city = city;
        }

        private String province;

        private String city;

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "Partition{" +
                    "province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
    }
}
