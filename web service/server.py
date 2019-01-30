# coding: utf-8
'''
http://soaplib.github.io/soaplib/2_0/pages/helloworld.html#declaring-a-soaplib-service
服务启动之后可以在浏览器： http://localhost:7789/?wsdl
'''
import soaplib
from soaplib.core.service import rpc, DefinitionBase, soap
from soaplib.core.model.primitive import String, Integer,Boolean
from soaplib.core.server import wsgi
from soaplib.core.model.clazz import Array


class HelloWorldService(DefinitionBase):
    @soap(String, Integer, _returns=Array(String))
    def say_hello(self, name, times):
        print name,times
        results = []
        for i in range(0, times):
            results.append('Hello, %s' % times)
        return results
    @soap(String, Integer)
    def auth(self,username,password):
        print "auth",username,password
        if username=="admin" and password==123:
            token="token:abc123"
            return 1
        else:
            return "Wrong username or password"
    @soap(Integer,_returns=String)
    def example(self,x):
        print "auth",x
        if x==123:
            token="token:abc123"
            return token
        else:
            return "Wrong username or password"


if __name__ == '__main__':
    try:
        from wsgiref.simple_server import make_server
        soap_application = soaplib.core.Application([HelloWorldService], 'abc')
        wsgi_application = wsgi.Application(soap_application)
        server = make_server('127.0.0.1', 7789, wsgi_application)
        print 'soap server starting......'
        server.serve_forever()
    except ImportError:
        print "Error: example server code requires Python >= 2.5"