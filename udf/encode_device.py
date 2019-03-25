#coding:utf-8
# encode_region.py

# https://help.aliyun.com/document_detail/73359.html?spm=5176.11065259.1996646101.searchclickresult.61324adfwSpWXx

from odps.udf import annotate
from odps.udf import BaseUDAF

# "android", "ipad", "iphone", "macintosh", "unknown", "windows_pc", "windows_phone"

@annotate('*->double')
class Average(BaseUDAF):
    
    def new_buffer(self):
        return [0, 0]
        
    def iterate(self, buffer, number):
        if number is not None:
            buffer[0] += number
            buffer[1] += 1
            
    def merge(self, buffer, pbuffer):
        buffer[0] += pbuffer[0]
        buffer[1] += pbuffer[1]
        
    def terminate(self, buffer):
        if buffer[1] == 0:
            return round(0.0, 0)
        return round(buffer[0] / buffer[1], 0)
