#coding:utf-8

from odps.udf import annotate

@annotate("*->bigint")
class Encode(object):
    def evaluate(self, arg0):
        encodes = {
            '20岁以下': 1, 
            '20-30岁': 2,
            '30-40岁': 3,
            '40-50岁': 4,
            '50岁以上': 5
        }
        result = 0
        if arg0 in encodes:
            result = encodes[arg0]
        return result
        
