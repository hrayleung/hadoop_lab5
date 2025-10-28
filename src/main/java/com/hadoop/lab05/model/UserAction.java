package com.hadoop.lab05.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * 用户行为数据模型
 * 对应JData_Action_*.csv文件的数据结构
 */
public class UserAction implements Writable {
    private String userId;
    private String skuId;
    private String time;
    private String modelId;
    private int type;
    private String cate;
    private String brand;

    public UserAction() {
    }

    public UserAction(String userId, String skuId, String time, String modelId,
            int type, String cate, String brand) {
        this.userId = userId;
        this.skuId = skuId;
        this.time = time;
        this.modelId = modelId;
        this.type = type;
        this.cate = cate;
        this.brand = brand;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId != null ? userId : "");
        out.writeUTF(skuId != null ? skuId : "");
        out.writeUTF(time != null ? time : "");
        out.writeUTF(modelId != null ? modelId : "");
        out.writeInt(type);
        out.writeUTF(cate != null ? cate : "");
        out.writeUTF(brand != null ? brand : "");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readUTF();
        skuId = in.readUTF();
        time = in.readUTF();
        modelId = in.readUTF();
        type = in.readInt();
        cate = in.readUTF();
        brand = in.readUTF();
    }

    // Getters and Setters
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSkuId() {
        return skuId;
    }

    public void setSkuId(String skuId) {
        this.skuId = skuId;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getCate() {
        return cate;
    }

    public void setCate(String cate) {
        this.cate = cate;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    @Override
    public String toString() {
        return userId + "," + skuId + "," + time + "," + modelId + "," +
                type + "," + cate + "," + brand;
    }

    /**
     * 获取行为类型描述
     */
    public String getTypeDescription() {
        switch (type) {
            case 1:
                return "浏览";
            case 2:
                return "加入购物车";
            case 3:
                return "购物车删除";
            case 4:
                return "下单";
            case 5:
                return "关注";
            case 6:
                return "点击";
            default:
                return "未知";
        }
    }
}
