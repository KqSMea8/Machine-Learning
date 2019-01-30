# coding: utf-8
import httplib
import xml.dom.minidom as dm


old_soap_body = '''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xmlns:xsd="http://www.w3.org/2001/XMLSchema"
xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
soap:encodingStyle="http://www.w3.org/2001/12/soap-encoding">
  <soap:Body>
    <tns:say_hello xmlns:tns="http://127.0.0.1:7789/">
        <tns:name>orangleliu</tns:name>
        <tns:times>10</tns:times>
    </tns:say_hello>
  </soap:Body>
</soap:Envelope>
'''

say_hello = '''<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" 
xmlns:ns1="tns" 
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
   <SOAP-ENV:Header/>
   <ns0:Body>
      <ns1:say_hello>
         <ns1:username>Dave</ns1:username>
         <ns1:password>5</ns1:password>
      </ns1:say_hello>
   </ns0:Body>
</SOAP-ENV:Envelope>
'''
auth = '''<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:ns0="http://schemas.xmlsoap.org/soap/envelope/" 
xmlns:ns1="tns" 
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
   <SOAP-ENV:Header/>
   <ns0:Body>
      <ns1:auth>
         <ns1:username>admin</ns1:username>
         <ns1:password>123</ns1:password>
      </ns1:auth>
   </ns0:Body>
</SOAP-ENV:Envelope>
'''

test = '''<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:a0="http://schemas.xmlsoap.org/soap/envelope/" 
xmlns:a1="abc" xmlns:a2="abc"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
   <SOAP-ENV:Header/>
   <SOAP-ENV:Body>
 
      <a2:auth>
         <a2:username>admin</a2:username>
         <a2:password>123</a2:password>
      </a2:auth>
      
           <a1:say_hello>
         <a1:name>Dave</a1:name>
         <a1:times>5</a1:times>
      </a1:say_hello>
   </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
'''

soap_host = '127.0.0.1'
soap_port = 7789
request_header = {'Content-Type': 'text/xml; charset=utf-8'}
conn = httplib.HTTPConnection(soap_host, soap_port, timeout=10)
conn.request('POST', '/', test.encode('utf-8'), request_header)
response = conn.getresponse()
data = response.read()
conn.close()
print data
# 格式化打印xml文档
xml = dm.parseString(data)
print xml.toprettyxml()

# import xml.etree.ElementTree as ET
#
# str_value = ET.fromstring(data)
# resultCode = str_value.find("authResult").text

# import xml.sax.handler
#
# class XMLHandler(xml.sax.handler.ContentHandler):
#     def __init__(self):
#         self.buffer = ""
#         self.mapping = {}
#
#     def startElement(self, name, attributes):
#         self.buffer = ""
#
#     def characters(self, data):
#         self.buffer += data
#
#     def endElement(self, name):
#         self.mapping[name] = self.buffer
#
#     def getDict(self):
#         return self.mapping
#
# xh = XMLHandler()
# xml.sax.parseString(data.encode(), xh)
# ret = xh.getDict()
# print ret.get("s0:authResult","")