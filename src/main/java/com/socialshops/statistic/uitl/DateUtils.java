//package com.socialshops.statistic.uitl;
//
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//
///**
// * Created by liyazhou on 2017/3/21.
// */
//public class DateUtils {
//
//    /**
//     *
//     *
//     * @param date, addDay
//     * @return
//     * @throws Exception
//     */
//    public static String getSpecifiedDay(Date date, int addDay) {
//        Calendar c = Calendar.getInstance();
//        try {
//            date = YYYYMMDD.parse(YYYYMMDD.format(date));
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        c.setTime(date);
//        int day = c.get(Calendar.DATE);
//        c.set(Calendar.DATE, day + addDay);
//
//        String dayBefore = YYYYMMDD.format(c
//                .getTime());
//        return dayBefore;
//    }
//
//    public static String getCurrentDay(Date date) {
//        return YYYYMMDD.format(date);
//    }
//}
