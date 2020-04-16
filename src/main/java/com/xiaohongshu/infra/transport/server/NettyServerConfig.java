package com.xiaohongshu.infra.transport.server;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;


import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.time.Duration;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class NettyServerConfig
{
    private static final int DEFAULT_WORKER_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;

    private int port;
    private int acceptBacklog = 1024;
    private int ioThreadCount = 3;
    private int workerThreadCount = DEFAULT_WORKER_THREAD_COUNT;
    private Integer maxFrameSize = 16*1024*1024;
    private Duration requestTimeout = Duration.ofMinutes(1);

    private Duration sslContextRefreshTime = Duration.ofMinutes(1);
    private boolean allowPlaintext = true;
    private boolean sslEnabled;
    private List<String> ciphers = ImmutableList.of();
    private File trustCertificate;
    private File key;
    private String keyPassword;
    private long sessionCacheSize = 10_000;
    private Duration sessionTimeout = Duration.ofDays(1);

    private boolean assumeClientsSupportOutOfOrderResponses = true;

    @Min(0)
    @Max(65535)
    public int getPort()
    {
        return port;
    }

    public NettyServerConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    @Min(0)
    public int getAcceptBacklog()
    {
        return acceptBacklog;
    }

    /**
     * Sets the number of pending connections that the {@link java.net.ServerSocket} will
     * queue up before the server process can actually accept them. If your server may take a lot
     * of connections in a very short interval, you'll want to set this higher to avoid rejecting
     * some of the connections. Setting this to 0 will apply an implementation-specific default.
     * <p>
     * The default value is 1024.
     *
     * Actual behavior of the socket backlog is dependent on OS and JDK implementation, and it may
     * even be ignored on some systems. See JDK docs
     * <a href="http://docs.oracle.com/javase/7/docs/api/java/net/ServerSocket.html#ServerSocket%28int%2C%20int%29" target="_top">here</a>
     * for details.
     */
    public NettyServerConfig setAcceptBacklog(int acceptBacklog)
    {
        this.acceptBacklog = acceptBacklog;
        return this;
    }

    public int getIoThreadCount()
    {
        return ioThreadCount;
    }

    public NettyServerConfig setIoThreadCount(int threadCount)
    {
        this.ioThreadCount = threadCount;
        return this;
    }

    public int getWorkerThreadCount()
    {
        return workerThreadCount;
    }

    public NettyServerConfig setWorkerThreadCount(int threadCount)
    {
        this.workerThreadCount = threadCount;
        return this;
    }

    public Integer getMaxFrameSize()
    {
        return maxFrameSize;
    }

    public NettyServerConfig setMaxFrameSize(Integer maxFrameSize)
    {
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    @NotNull
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    public NettyServerConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public boolean isAllowPlaintext()
    {
        return allowPlaintext;
    }

    public NettyServerConfig setAllowPlaintext(boolean allowPlaintext)
    {
        this.allowPlaintext = allowPlaintext;
        return this;
    }

    public Duration getSslContextRefreshTime()
    {
        return sslContextRefreshTime;
    }

    public NettyServerConfig setSslContextRefreshTime(Duration sslContextRefreshTime)
    {
        this.sslContextRefreshTime = sslContextRefreshTime;
        return this;
    }

    public boolean isSslEnabled()
    {
        return sslEnabled;
    }

    public NettyServerConfig setSslEnabled(boolean sslEnabled)
    {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public File getTrustCertificate()
    {
        return trustCertificate;
    }

    public NettyServerConfig setTrustCertificate(File trustCertificate)
    {
        this.trustCertificate = trustCertificate;
        return this;
    }

    public File getKey()
    {
        return key;
    }

    public NettyServerConfig setKey(File key)
    {
        this.key = key;
        return this;
    }

    public String getKeyPassword()
    {
        return keyPassword;
    }

    public NettyServerConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public long getSessionCacheSize()
    {
        return sessionCacheSize;
    }

    public NettyServerConfig setSessionCacheSize(long sessionCacheSize)
    {
        this.sessionCacheSize = sessionCacheSize;
        return this;
    }

    public Duration getSessionTimeout()
    {
        return sessionTimeout;
    }

    public NettyServerConfig setSessionTimeout(Duration sessionTimeout)
    {
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    public List<String> getCiphers()
    {
        return ciphers;
    }

    public NettyServerConfig setCiphers(String ciphers)
    {
        this.ciphers = Splitter
                .on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(requireNonNull(ciphers, "ciphers is null"));
        return this;
    }

    public boolean isAssumeClientsSupportOutOfOrderResponses()
    {
        return assumeClientsSupportOutOfOrderResponses;
    }

    public NettyServerConfig setAssumeClientsSupportOutOfOrderResponses(boolean assumeClientsSupportOutOfOrderResponses)
    {
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
        return this;
    }
}
