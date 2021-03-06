package com.atguigu.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {
    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType , String tableName , List<CanalEntry.RowData> rowDataList){
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle(){
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT == eventType){
            sendToKafka(GmallConstant.KAFKA_TOPIC_ORDER);
        }
    }

    public void sendToKafka(String topic){
        for (CanalEntry.RowData rowData : rowDataList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();

            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "--->" + column.getValue());
                jsonObject.put(column.getName() , column.getValue());
            }

            KafkaSender.send(topic , jsonObject.toJSONString());

        }
    }
}
