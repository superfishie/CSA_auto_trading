# -*- coding:utf-8 -*-
# Editor: LIUQIANG

from datetime import datetime
import pytz
import time
from datetime import timedelta


class ToolDateTime(object):
    @staticmethod
    def get_date_string(param="s"):
        """
        Get ZMQ string datetime
        Convert standard datetime to millisecond string
        Convert standard datetime to microsecond string
        eg: 2018-08-29T09-38-19-776474Z
        :param param: "s":秒,"ms"：毫秒, "us" 微秒 "ym"：年月,"ymd"：年月日
        :return:
        """
        string_date_time = datetime.now().isoformat()
        string_date_time = string_date_time.replace(":", "-")
        string_date_time = string_date_time.replace(".", "-")
        if param == "us":
            if len(string_date_time) == 19:
                string_date_time += "-0"
            string_date_time = string_date_time + "Z"
            return string_date_time
        elif param == 'ms':
            if len(string_date_time) == 19:
                string_date_time += "-0"
            string_date_time = string_date_time[:23] + "Z"
            return string_date_time
        elif param == "s":
            string_date_time = string_date_time[:19]
            string_date_time = string_date_time + "Z"
            return string_date_time
        elif param == "ym":
            string_date_time = string_date_time[:7]
            return string_date_time
        elif param == "ymd":
            string_date_time = string_date_time[:10]
            return string_date_time

    @staticmethod
    def get_date_int(param='s'):
        """
        Get ZMQ int datetime
        :param param:  "s":秒,"ms"：毫秒, "us" 微秒
        :return:
        """
        td = datetime.now() - datetime.fromtimestamp(0)
        time_stamp = td.microseconds + (td.seconds + td.days * 86400) * 10 ** 6
        if param == 's':
            time_stamp = int(time_stamp / 10 ** 6)
        elif param == 'ms':
            time_stamp = int(time_stamp / 10 ** 3)
        elif param == 'us':
            time_stamp = int(time_stamp)
        elif param == 'ns':
            time_stamp = int(time_stamp * 10 ** 3)
        return time_stamp

    @staticmethod
    def int_to_datetime(date_time_int):
        date_time_str = str(date_time_int)
        if '.' in date_time_str:
            result = datetime.fromtimestamp(date_time_int)
        else:
            length = len(date_time_str)
            if length == 10:
                result = datetime.fromtimestamp(date_time_int)
            elif length == 13:
                result = datetime.fromtimestamp(date_time_int/10 ** 3)
            elif length == 16:
                result = datetime.fromtimestamp(date_time_int / 10 ** 6)
        return result

    @staticmethod
    def int_to_string(date_time_int, param=None):
        """
        Convert int to string
         eg: 1534492036898632  ->  2018-08-17T15-47-16-898632Z
             1534492036898632  ->  2018-08-17T15-47-16Z
        :param date_time_int: 16bit int
        :param param: 's', 'us', 'ms'
        :return:
        """
        time_length = len(str(int(date_time_int)))
        date_time_int = int(date_time_int)
        if time_length < 10:
            raise Exception("输入的时间戳长度小于10位")
        if time_length < 16:
            date_time_int = date_time_int * (10 ** (16 - time_length))
        date = date_time_int / 1000000.00
        date_time = datetime.fromtimestamp(date)
        string_date_time = date_time.isoformat()
        if param:
            if param == 'ms':
                string_date_time = string_date_time[:23]
            elif param == 's':
                string_date_time = string_date_time[:19]
        else:
            if 10 <= time_length < 13:
                string_date_time = string_date_time[:19]
            elif 13 <= time_length < 16:
                string_date_time = string_date_time[:23]
        string_date_time = string_date_time.replace(":", "-")
        string_date_time = string_date_time.replace(".", "-")
        string_date_time = string_date_time + "Z"
        return string_date_time

    @staticmethod
    def string_to_datetime(date_time_string):
        """
        2018-09-06T10-15-27-888209Z -> date_time (2018-09-06 10:15:27.888209)
        :param date_time_string:
        :return:
        """
        tmp = date_time_string.split("T")[1]
        if len(tmp.split("-")) > 3:
            datetime_object = datetime.strptime(date_time_string, '%Y-%m-%dT%H-%M-%S-%fZ')
        else:
            datetime_object = datetime.strptime(date_time_string, '%Y-%m-%dT%H-%M-%SZ')
        return datetime_object

    @staticmethod
    def datetime_to_string(date_time, param="s"):
        string_date_time = date_time.isoformat()
        string_date_time = string_date_time.replace(":", "-")
        string_date_time = string_date_time.replace(".", "-")
        if param == "ms":
            if len(string_date_time) == 19:
                string_date_time += "-0"
            string_date_time = string_date_time + "Z"
            return string_date_time
        elif param == "s":
            string_date_time = string_date_time[:19]
            string_date_time = string_date_time + "Z"
            return string_date_time
        elif param == "ym":
            string_date_time = string_date_time[:7]
            return string_date_time
        elif param == "ymd":
            string_date_time = string_date_time[:10]
            return string_date_time

    @staticmethod
    def string_to_int(date_time_string, param=None):
        """
         ZMQ string to int
        :param date_time_string:
        :param param: number, s ,ms, us
        :return:
        """
        epoch = datetime.fromtimestamp(0)
        if len(date_time_string.split("-")) == 6:
            datetime_object = datetime.strptime(date_time_string, '%Y-%m-%dT%H-%M-%S-%fZ')
            td = datetime_object - epoch
            timestamp = td.microseconds + (td.seconds + td.days * 86400) * 10 ** 6
        elif len(date_time_string.split("-")) == 5:
            datetime_object = datetime.strptime(date_time_string, '%Y-%m-%dT%H-%M-%SZ')
            td = datetime_object - epoch
            timestamp = td.microseconds + (td.seconds + td.days * 86400) * 10 ** 6
        else:
            raise Exception('Invalid string datetime, please use ZMQ format')
        if param:
            if param == 's':
                int_param = 10
            elif param == 'ms':
                int_param = 13
            elif param == 'us':
                int_param = 16
            return timestamp / 10 ** (16 - int_param)
        else:
            length = len(date_time_string)
            if length == 20:
                timestamp = timestamp / 10 ** 6
            elif length == 24:
                timestamp = timestamp / 10 ** 3
            return timestamp

    @staticmethod
    def int_to_date_zone(date_time_int, zone_str="UTC"):
        """
        2019-09-04T03:42:31.205159+00:00
        :param date_time_int: 16bit int
        :param zone_str: zone
        :return:
        """
        date = date_time_int / 1000000.00
        date_time = datetime.fromtimestamp(date, pytz.timezone(zone_str)).isoformat()
        return date_time

    @staticmethod
    def int_to_string_zone(date_time_int, zone_str="UTC"):
        date = date_time_int / 1000000.00
        date_time = datetime.fromtimestamp(date, pytz.timezone(zone_str))
        return date_time.isoformat()

    @staticmethod
    def date_rounding_up(date_time):
        """
        Rounding millisecond string up to second string
        eg: 2018-08-17T15-47-16-898632Z  -> 2018-08-17T15-47-17Z
        :param date_time:
        :return:
        """
        # timestamp = ToolDateTime.string_to_int(date_time)
        timestamp_s = ToolDateTime.string_to_int(date_time, 's') + 1
        return ToolDateTime.int_to_string(timestamp_s, "s")

    @staticmethod
    def time_step(date_time_string, step=1, format="s"):
        """
        返回前进一秒钟后的字符串时间格式
        :param date_time_string:
        :param step:  int number -1,0,1
        :param format:  "s","min","hour"
        :return date_time_string:
        """
        length = len(date_time_string)
        if 20 <= length < 24:
            param = 's'
        elif 24 <= length < 27:
            param = 'ms'
        else:
            param = 'us'
        timestamp = ToolDateTime.string_to_int(date_time_string, 'us')
        if format == "s":
            timestamp += int(step) * 10 ** 6
        elif format == "min":
            timestamp += int(step) * 60 * 10 ** 6
        elif format == "hour":
            timestamp += int(step) * 60 * 60 * 10 ** 6
        elif format == "day":
            timestamp += int(step) * 24 * 60 * 60 * 10 ** 6
        date_time_string = ToolDateTime.int_to_string(timestamp, param)
        return date_time_string

    @staticmethod
    def get_delta_time(date_time_a, date_time_b, format='string'):
        """
        date_time_string_a - date_time_string_b
        返回两时间的差值，以秒为单位
        :param date_time_a: "2018-08-17T15-47-16Z"
        :param date_time_b: "2018-08-17T15-47-15Z"
        :param format: "string" or 'datetime'
        :return:
        """
        if format == 'string':
            dif = ToolDateTime.string_to_int(date_time_a, 'us') - ToolDateTime.string_to_int(date_time_b, 'us')
        elif format == 'datetime':
            string_a = ToolDateTime.datetime_to_string(date_time_a)
            string_b = ToolDateTime.datetime_to_string(date_time_b)
            dif = ToolDateTime.string_to_int(string_a, 'us') - ToolDateTime.string_to_int(string_b, 'us')
        return dif / 10.0 ** 6

    @staticmethod
    def get_hourly_chime(dt, step=0, rounding_level="s"):
        """
        计算整分钟，整小时，整天的时间
        :param step: 往前或往后跳跃取整值，默认为0，即当前所在的时间，正数为往后，负数往前。
                    例如：
                    step = 0 时 2019-04-11 17:38:21.869993 取整秒后为 2019-04-11 17:38:21
                    step = 1 时 2019-04-11 17:38:21.869993 取整秒后为 2019-04-11 17:38:22
                    step = -1 时 2019-04-11 17:38:21.869993 取整秒后为 2019-04-11 17:38:20
        :param rounding_level: 字符串格式。
                    "s": 按秒取整；"min": 按分钟取整；"hour": 按小时取整；"days": 按天取整
        :return: 整理后的时间戳
        """
        if isinstance(dt, str):
            dt = ToolDateTime.string_to_datetime(dt)
        if rounding_level == "days":  # 整天
            td = timedelta(days=-step, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=dt.hour,
                           weeks=0)
            new_dt = dt - td
        elif rounding_level == "hour":  # 整小时
            td = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=-step, weeks=0)
            new_dt = dt - td
        elif rounding_level == "min":  # 整分钟
            td = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=-step, hours=0, weeks=0)
            new_dt = dt - td
        elif rounding_level == "s":  # 整秒
            td = timedelta(days=0, seconds=-step, microseconds=dt.microsecond, milliseconds=0, minutes=0, hours=0, weeks=0)
            new_dt = dt - td
        else:
            new_dt = dt
        # timestamp1 = new_dt.timestamp()  # 对于 python 3 可以直接使用 timestamp 获取时间戳
        timestamp = (new_dt - datetime.fromtimestamp(0)).total_seconds()  # Python 2 需收到换算
        return timestamp

    @staticmethod
    def format_time(date_time_string, format):
        """

        :param date_time_string:
        :param format:
        :return:
        """
        if format == 'y':
            return date_time_string[:4]
        elif format == 'ym':
            return date_time_string[:7]
        elif format == 'ymd':
            return date_time_string[:10]
        elif format == 'ymdz':
            return date_time_string[:10] + 'T00-00-00Z'
        return


if __name__ == '__main__':
    a = ToolDateTime.get_date_int('ns')
    print(a)
    # print(ToolDateTime.int_to_datetime(a))
    # a = 1567658165.234
    # b = datetime.fromtimestamp(a)
    # c = ToolDateTime.int_to_string(a)
    # print(a, b, c)
    #
    # datetime.fromtimestamp()
    # # a = ToolDateTime.get_date_string()
    # # c = ToolDateTime.string_to_datetime(a)
    # b = ToolDateTime.int_to_string(a)
    # c = ToolDateTime.get_hourly_chime(b, 1, 'min')
    #
    # print(b, ToolDateTime.int_to_string(c))
    # print(a, c, c-a)
    # a = 1567506669000000
    # a = ToolDateTime.get_date_string('s')
    # print a
    # print ToolDateTime.time_step(a, 1, 'min')
    #
    # a = ToolDateTime.get_date_string('ms')
    # print a
    # print ToolDateTime.time_step(a, 1, 'min')
    #
    # a = ToolDateTime.get_date_string('us')
    # print a
    # print ToolDateTime.time_step(a, 1, 'min')

    # a = ToolDateTime.get_date_int()
    #
    # print ToolDateTime.date_rounding_up(ToolDateTime.get_date_string())
    # print ToolDateTime.int_to_date_zone(a)
    #
    #
    # b = ToolDateTime.int_to_string(a)
    # c = ToolDateTime.string_to_int(b)
    # d = ToolDateTime.string_to_int(b, 13)
    #
    # a = ToolDateTime.get_date_string('s')
    # print ToolDateTime.string_to_int(a)
    # print a,b,c,d

    #
    # a = ToolDateTime.get_date_int('s')
    # print a
    # print ToolDateTime.int_to_string(a)
    # print ToolDateTime.int_to_string(a, 's')
    # b = ToolDateTime.get_date_int('us')
    # print b
    # print ToolDateTime.int_to_string(b)
    # a = ToolDateTime.string_to_int('2018-09-02T00-00-00Z')
    # b = ToolDateTime.string_to_int1('2018-09-02T00-00-00Z')
    # print a,b
    # a = datetime.fromtimestamp(0)
    # print a
    # a = ToolDateTime.string_to_int('2018-09-02T00-00-00Z')
    # a = datetime.now()
    # a = (datetime.now() - datetime.fromtimestamp(0))
    # b = a.total_seconds()
    #
    # b = (datetime.now() - datetime(1970,1,1)).total_seconds()
    # c = ToolDateTime.int_to_string(a)
    # d = ToolDateTime.int_to_string(b)
    # e = ToolDateTime.int_to_string(b, 'utc')
    # print (time.time())
    #
    # print(ToolDateTime.get_date_int())
    # # print(ToolDateTime.get_date_string('ms'))
    # print(ToolDateTime.int_to_string(1565162885702))
