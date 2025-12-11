
/*
 * 文件名: RedisComponent.cs
 * 作者: zengxin
 * 创建日期: 2025-12-11
 */

using System;
using System.Collections.Generic;


namespace ET
{
    /// <summary>
    /// Redis组件配置
    /// </summary>
    [ComponentOf(typeof(Scene))]
    public class RedisComponent : Entity, IAwake, IDestroy
    {
        // Redis连接池
        public RedisConnectionPool ConnectionPool { get; set; }
        
        // 配置信息
        public string ConnectionString { get; set; } = 
            "localhost:6379,password=,defaultDatabase=0,connectTimeout=5000,syncTimeout=5000,abortConnect=false";
        
        // 池配置
        public int MaxPoolSize { get; set; } = 50;
        public int MinPoolSize { get; set; } = 5;
        public int CommandTimeout { get; set; } = 5000;
        
        // 重试配置
        public int MaxRetryCount { get; set; } = 3;
        public int RetryDelayMs { get; set; } = 100;
        public bool AutoReconnect { get; set; } = true;
        
        // 统计信息
        public long TotalOperations;
        public long FailedOperations;
        public long SuccessfulOperations;
        public long TotalOperationTime;
        public long TotalConnectionsCreated;
        public long TotalConnectionsClosed;
        public long TotalConnectionWaitTime;
        public long TotalReconnects;
        
        // 性能监控
        public DateTime LastOperationTime { get; set; }
        public DateTime LastErrorTime { get; set; }
        public string LastErrorMessage { get; set; }
        public long CurrentConnections => ConnectionPool?.ActiveConnections ?? 0;
        
        // 缓存统计
        public long CacheHits;
        public long CacheMisses;
        public long CacheEvictions;
        
        // Timer
        public long HealthCheckTimer;
        public long StatisticsTimer;
        public long MetricsTimer;
        
        // 分布式锁配置
        public int DefaultLockTimeout { get; set; } = 30000; // 30秒
        public int DefaultLockRetryCount { get; set; } = 3;
        public int DefaultLockRetryDelay { get; set; } = 100;
        
        // 发布订阅
        public bool EnablePubSub { get; set; } = true;
        public List<string> SubscribeChannels { get; set; } = new();
    }
}