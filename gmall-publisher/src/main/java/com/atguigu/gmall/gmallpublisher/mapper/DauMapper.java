package com.atguigu.gmall.gmallpublisher.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

//@Mapper
public interface DauMapper {

    public Long selectDauCount(String date);

    public List<Map> selectDauCountHour(String date);
}
