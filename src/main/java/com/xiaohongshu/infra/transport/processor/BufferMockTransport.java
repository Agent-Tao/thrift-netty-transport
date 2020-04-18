/*
 * Copyright (C) 2012-2016 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xiaohongshu.infra.transport.processor;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * @Author: JiangTao
 * @Date: Created on 下午7:08 2018/3/15.
 * @Description:
 */

public class BufferMockTransport extends TTransport {

    byte[] bufs_;
    int pos_ = 0;

    public BufferMockTransport(byte[] bufs, int pos) {
        bufs_ = bufs;
        pos_ = pos;
    }

    public void resetPos() {
        pos_ = 0;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void open() throws TTransportException {

    }

    @Override
    public void close() {

    }

    @Override
    public int read(byte[] bytes, int s, int e) throws TTransportException {
        int j=0;
        for(int i=s;i<e;i++,j++) {
            bytes[j]=bufs_[i];
        }
        return j;
    }

    @Override
    public void write(byte[] bytes, int s, int e) throws TTransportException {
        int j=s+pos_;
        for(int i=s;i<e;i++,j++) {
            bufs_[j] = bytes[i];
        }
        pos_+=(e-s);
    }

    @Override
    public byte[] getBuffer() {
        return bufs_;
    }

    @Override
    public int getBufferPosition() {
        return pos_;
    }

    @Override
    public int getBytesRemainingInBuffer() {
        return bufs_.length - pos_;
    }

    @Override
    public void consumeBuffer(int len) {
        pos_ += len;
    }
}
