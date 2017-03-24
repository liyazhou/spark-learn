package com.socialshops.test;

import com.alibaba.fastjson.JSON;
import com.socialshops.event.Constant;
import com.socialshops.event.EventEnum;
import com.socialshops.event.saas.buyer.CreateOrderEvent;
import com.socialshops.event.saas.buyer.LoginEvent;
import org.junit.Test;

import java.text.ParseException;
import java.util.Date;

import static com.socialshops.event.Constant.YYYYMMDDHHMMSS_FORMAT;

/**
 * Created by liyazhou on 2017/3/22.
 */
public class TestClass {
    @Test
    public void test() {
        for (int i = 0; i < 500; i++) {
            LoginEvent loginEvent = new LoginEvent();
            loginEvent.setBuyerUserId("uu13432");
            loginEvent.setMobileAppId("mobile_seller_yixun");
//            loginEvent.setSellerUserId("test");
            loginEvent.setLoginDate(new Date());
            loginEvent.recordToLog();
        }
        CreateOrderEvent createOrderEvent = new CreateOrderEvent();
        createOrderEvent.setBuyerUserId("yazhou");
        createOrderEvent.setTotalPrice(32432.23432);
        createOrderEvent.setMobileAppId("yixun");
        createOrderEvent.setCreateDate(new Date());
        createOrderEvent.recordToLog();
    }

    @Test
    public void testParse() {
        String test = "691d38222c6744bc88851be047c3257b\u00012017-03-20 13:26:21\u00011001\u0001{\"business\":\"SAAS\",\"buyerUserId\":\"yazhou\",\"createDate\":1489987581433,\"eventType\":\"1001\",\"mobileAppId\":\"yixun\",\"totalPrice\":32432.23432}";
        String[] array = test.split(Constant.LOG_SEPARATOR);
        try {
            Date date = YYYYMMDDHHMMSS_FORMAT.parse(array[1]);
            System.out.println(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        CreateOrderEvent createOrderEvent = (CreateOrderEvent) JSON.parseObject(array[3], EventEnum.EVENT_TYPE_MAP.get(array[2]));
        System.out.println(createOrderEvent);


    }

}
