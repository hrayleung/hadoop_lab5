package com.hadoop.lab05.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * 用户档案数据模型
 * 对应JData_User.csv文件的数据结构
 */
public class UserProfile implements Writable {
    private String userId;
    private String age;
    private int sex;
    private int userLvCd;
    private String userRegTm;

    public UserProfile() {
    }

    public UserProfile(String userId, String age, int sex, int userLvCd, String userRegTm) {
        this.userId = userId;
        this.age = age;
        this.sex = sex;
        this.userLvCd = userLvCd;
        this.userRegTm = userRegTm;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId != null ? userId : "");
        out.writeUTF(age != null ? age : "");
        out.writeInt(sex);
        out.writeInt(userLvCd);
        out.writeUTF(userRegTm != null ? userRegTm : "");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readUTF();
        age = in.readUTF();
        sex = in.readInt();
        userLvCd = in.readInt();
        userRegTm = in.readUTF();
    }

    // Getters and Setters
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public int getUserLvCd() {
        return userLvCd;
    }

    public void setUserLvCd(int userLvCd) {
        this.userLvCd = userLvCd;
    }

    public String getUserRegTm() {
        return userRegTm;
    }

    public void setUserRegTm(String userRegTm) {
        this.userRegTm = userRegTm;
    }

    @Override
    public String toString() {
        return userId + "," + age + "," + sex + "," + userLvCd + "," + userRegTm;
    }

    /**
     * 获取性别描述
     */
    public String getSexDescription() {
        switch (sex) {
            case 0:
                return "男";
            case 1:
                return "女";
            case 2:
                return "保密";
            default:
                return "未知";
        }
    }
}
