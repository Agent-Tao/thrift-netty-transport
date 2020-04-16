package com.xiaohongshu.infra.test.api;

import org.apache.thrift.TException;

/**
 * Created by chenshunyang on 2016/10/30.
 */
public class HelloServiceImpl implements HelloService.Iface{
    public String hello(String name) throws TException {
        System.out.println(name);
        return "hello " + name;
    }
}