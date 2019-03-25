#coding:utf-8

from odps.udf import annotate

@annotate("*->bigint")
class Encode(object):
    def evaluate(self, arg0, arg1):
        encodes = {
            "白羊座":1, 
            "金牛座":2, 
            "双子座":3, 
            "巨蟹座":4, 
            "狮子座":5, 
            "处女座":6, 
            "天秤座":7, 
            "天蝎座":8, 
            "射手座":9, 
            "摩羯座":10, 
            "水瓶座":11, 
            "双鱼座":12
        }
        result = 0
        if arg0 in encodes:
            result = encodes[arg0]
        return result
        
