package com.atguigu.gmall.gmallpublisher.controller;


import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;


    @GetMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String date){
        Long dataCount = publisherService.getDateCount(date);
        Double orderAmount = publisherService.getOrderAmount(date);

        List<Map> totalList = new ArrayList<>();
        Map dauMap = new HashMap<String,Object>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dataCount);
        totalList.add(dauMap);

        Map orderAmountMap=new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value",orderAmount);
        totalList.add(orderAmountMap);

        return JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id , @RequestParam("date") String date){

        if("dau".equals(id)) {
            Map dateCountHourTd = publisherService.getDateCountHour(date);
            String yd = getYd(date);
            Map dateCountHourYd = publisherService.getDateCountHour(yd);

            Map<String, Map> map = new HashMap<>();
            map.put("yesterday" , dateCountHourYd);
            map.put("today" , dateCountHourTd);
            return JSON.toJSONString(map);

        }else if ("order-amount".equals(id)){
            Map OrderAmountHourTd = publisherService.getOrderAmountHour(date);
            String yd = getYd(date);
            Map OrderAmountHourYd = publisherService.getOrderAmountHour(yd);

            Map<String, Map> map = new HashMap<>();
            map.put("yesterday" , OrderAmountHourYd);
            map.put("today" , OrderAmountHourTd);
            return JSON.toJSONString(map);
        }
        else{
            return null;
        }
    }

    private String getYd(String date){
        String Yd = null;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date today = formatter.parse(date);
            Date yesterday = DateUtils.addDays(today, -1);
            Yd = formatter.format(yesterday);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return Yd;
    }
}
