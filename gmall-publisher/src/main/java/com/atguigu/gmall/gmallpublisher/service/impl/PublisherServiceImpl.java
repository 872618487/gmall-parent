package com.atguigu.gmall.gmallpublisher.service.impl;

import com.atguigu.gmall.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmall.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmall.gmallpublisher.service.PublisherService;
import org.apache.jasper.tagplugins.jstl.ForEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Long getDateCount(String date) {
        return dauMapper.selectDauCount(date);
    }

    @Override
    public Map getDateCountHour(String date) {
        Map resultMap = new HashMap();
        List<Map> maps = dauMapper.selectDauCountHour(date);
        for (Map map : maps) {
            resultMap.put(map.get("hour") , map.get("ct"));
        }
        return resultMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        Map resultMap = new HashMap();
        List<Map> maps = orderMapper.selectOrderAmountHour(date);
        for (Map map : maps) {
            resultMap.put(map.get("CREATE_HOUR") , map.get("SUM_AMOUNT"));
        }
        return resultMap;
    }

}
