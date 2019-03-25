#coding:utf-8
# encode_region.py

# https://help.aliyun.com/document_detail/73359.html?spm=5176.11065259.1996646101.searchclickresult.61324adfwSpWXx

from odps.udf import annotate
from odps.udf import BaseUDTF

@annotate('string,string->string,bigint,bigint')
class Encode(BaseUDTF):
    """将string按逗号分隔输出成多条记录"""
    
    def process(self, arg0, arg1):
        uid = arg0 
        provience = arg1 

        if provience == "广西":
            provience = "广西省"

        proviences = [
            "上海市", "北京市", "天津市", "重庆市", "澳门", "香港", "台湾省",
            "云南省", "内蒙古", "吉林省", "四川省", "宁夏", "安徽省", "山东省", "山西省", "广西省", "广东省", 
            "新疆", "江苏省", "江西省", "河北省", "河南省", "浙江省", "海南省", "湖北省", "湖南省", "甘肃省", 
            "福建省", "贵州省", "辽宁省", "陕西省", "青海省", "黑龙江省"
        ]

        districts = ["上海市", "北京市", "天津市", "重庆市", "澳门", "香港", "台湾省"]

        e1 = 0
        if provience in proviences:
            e1 = proviences.index(provience)

        e2 = 0
        if provience in proviences:
            e2 = 1
            if provience in districts:
                e2 = 2
        
        proc = [uid, e1, e2]
        self.forward(*proc)



'''
@annotate('string->bigint,bigint')
class Encode(BaseUDTF):
    """将string按逗号分隔输出成多条记录"""
    
    def process(self, arg):
        provience = arg 
        if provience == "广西":
            provience = "广西省"

        proviences = [
            "上海市", "北京市", "天津市", "重庆市", "澳门", "香港", "台湾省",
            "云南省", "内蒙古", "吉林省", "四川省", "宁夏", "安徽省", "山东省", "山西省", "广西省", "广东省", 
            "新疆", "江苏省", "江西省", "河北省", "河南省", "浙江省", "海南省", "湖北省", "湖南省", "甘肃省", 
            "福建省", "贵州省", "辽宁省", "陕西省", "青海省", "黑龙江省"
        ]

        districts = ["上海市", "北京市", "天津市", "重庆市", "澳门", "香港", "台湾省"]

        e1 = 0
        if provience in proviences:
            e1 = proviences.index(provience)

        e2 = 0
        if provience in proviences:
            e2 = 1
            if provience in districts:
                e2 = 2
        
        proc = [e1, e2]
        self.forward(*proc)
'''
