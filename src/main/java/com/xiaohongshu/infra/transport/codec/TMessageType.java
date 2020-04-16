package com.xiaohongshu.infra.transport.codec;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: jiangtao
 * Date: 2020-04-16
 * Time: 下午5:52
 */
public final class TMessageType
{
    public static final byte CALL = 1;
    public static final byte REPLY = 2;
    public static final byte EXCEPTION = 3;
    public static final byte ONEWAY = 4;

    private TMessageType() {}
}
