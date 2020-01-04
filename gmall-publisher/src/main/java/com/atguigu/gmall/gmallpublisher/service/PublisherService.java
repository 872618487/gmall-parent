package com.atguigu.gmall.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {

    public Long getDateCount(String date);

    public Map getDateCountHour(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);
}
