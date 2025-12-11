/*
 * 文件名: RedisSystem.cs
 * 作者: zengxin
 * 创建日期: 2025-12-11
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace ET
{
    [EntitySystemOf(typeof(RedisComponent))]
    [FriendOf(typeof(RedisComponent))]
    public static partial class RedisSystem
    {
        // Timer定义
        [Invoke(TimerInvokeType.RedisHealthCheck)]
        public class RedisHealthCheckTimer : ATimer<RedisComponent>
        {
            protected override void Run(RedisComponent self)
            {
                try
                {
                    self.ConnectionPool?.RunHealthCheckAsync().Coroutine();
                }
                catch (Exception e)
                {
                    Log.Error($"Redis健康检查异常: {self.Id}\n{e}");
                    self.LastErrorMessage = e.Message;
                    self.LastErrorTime = DateTime.Now;
                }
            }
        }
        
        [Invoke(TimerInvokeType.RedisStatistics)]
        public class RedisStatisticsTimer : ATimer<RedisComponent>
        {
            protected override void Run(RedisComponent self)
            {
                try
                {
                    self.LogStatistics();
                }
                catch (Exception e)
                {
                    Log.Error($"Redis统计异常: {self.Id}\n{e}");
                }
            }
        }
        
        [Invoke(TimerInvokeType.RedisMetrics)]
        public class RedisMetricsTimer : ATimer<RedisComponent>
        {
            protected override void Run(RedisComponent self)
            {
                try
                {
                    self.CollectMetricsAsync().Coroutine();
                }
                catch (Exception e)
                {
                    Log.Error($"Redis指标收集异常: {self.Id}\n{e}");
                }
            }
        }
        
        [EntitySystem]
        private static void Awake(this RedisComponent self)
        {
            try
            {
                Log.Debug("RedisComponent Awake");
                
                // 确保TimerComponent存在
                var timerComponent = self.Scene().GetComponent<TimerComponent>();
                if (timerComponent == null)
                {
                    timerComponent = self.Scene().AddComponent<TimerComponent>();
                }
                
                // 启动定时器
                self.HealthCheckTimer = timerComponent.NewRepeatedTimer(30000, TimerInvokeType.RedisHealthCheck, self);
                self.StatisticsTimer = timerComponent.NewRepeatedTimer(60000, TimerInvokeType.RedisStatistics, self);
                self.MetricsTimer = timerComponent.NewRepeatedTimer(10000, TimerInvokeType.RedisMetrics, self);
                
                Log.Info($"Redis组件已初始化，配置: MaxPoolSize={self.MaxPoolSize}, MinPoolSize={self.MinPoolSize}");
            }
            catch (Exception ex)
            {
                Log.Error($"Redis组件Awake异常: {ex}");
                throw;
            }
        }

        [EntitySystem]
        private static void Destroy(this RedisComponent self)
        {
            try
            {
                // 取消定时器
                var timerComponent = self.Scene().GetComponent<TimerComponent>();
                if (timerComponent != null)
                {
                    timerComponent.Remove(ref self.HealthCheckTimer);
                    timerComponent.Remove(ref self.StatisticsTimer);
                    timerComponent.Remove(ref self.MetricsTimer);
                }
                
                // 销毁连接池
                self.ConnectionPool?.Dispose();
                self.ConnectionPool = null;
                
                // 记录统计信息
                self.LogStatistics("组件销毁");
                
                Log.Info("Redis组件已销毁");
            }
            catch (Exception ex)
            {
                Log.Error($"Redis组件销毁异常: {ex.Message}");
            }
        }
        
        // ========== 初始化方法 ==========
        
        public static async ETTask InitializeAsync(this RedisComponent self)
        {
            try
            {
                Log.Info("Redis初始化开始...");
                
                // 1. 初始化连接池
                self.ConnectionPool = new RedisConnectionPool(
                    self.Scene(),
                    self.ConnectionString, 
                    self.MaxPoolSize,
                    self.MinPoolSize);
                
                // 2. 预热连接
                await WarmupPool(self);
                
                // 3. 验证连接
                await ValidateConnection(self);
                
                // 4. 初始化发布订阅
                if (self.EnablePubSub && self.SubscribeChannels.Count > 0)
                {
                    await InitializePubSub(self);
                }
                
                Log.Info($"Redis初始化完成，池大小: {self.MinPoolSize}-{self.MaxPoolSize}");
            }
            catch (Exception ex)
            {
                Log.Error($"Redis初始化失败: {ex.Message}");
                self.LastErrorMessage = ex.Message;
                self.LastErrorTime = DateTime.Now;
                throw;
            }
        }
        
        /// <summary>
        /// 重新加载配置
        /// </summary>
        public static async ETTask ReloadConfig(this RedisComponent self)
        {
            try
            {
                Log.Info("Redis配置重载中...");
                
                var oldPool = self.ConnectionPool;
                
                self.ConnectionPool = new RedisConnectionPool(
                    self.Scene(),
                    self.ConnectionString, 
                    self.MaxPoolSize,
                    self.MinPoolSize);
                
                await WarmupPool(self);
                
                if (oldPool != null)
                {
                    oldPool.Dispose();
                }
                
                Log.Info("Redis配置重载完成");
            }
            catch (Exception ex)
            {
                Log.Error($"Redis配置重载失败: {ex.Message}");
                self.LastErrorMessage = ex.Message;
                self.LastErrorTime = DateTime.Now;
                throw;
            }
        }
        
        // ========== 统计和状态方法 ==========
        
        /// <summary>
        /// 记录操作开始
        /// </summary>
        public static (long, Stopwatch) BeginOperation(this RedisComponent self)
        {
            Interlocked.Increment(ref self.TotalOperations);
            self.LastOperationTime = DateTime.Now;
            var stopwatch = Stopwatch.StartNew();
            return (Environment.TickCount64, stopwatch);
        }
        
        /// <summary>
        /// 记录操作完成
        /// </summary>
        public static void EndOperation(this RedisComponent self, 
            (long startTime, Stopwatch stopwatch) opInfo, bool success, bool isHit = false)
        {
            var (startTime, stopwatch) = opInfo;
            stopwatch.Stop();
            
            Interlocked.Add(ref self.TotalOperationTime, stopwatch.ElapsedMilliseconds);
            
            if (success)
            {
                Interlocked.Increment(ref self.SuccessfulOperations);
                
                if (isHit)
                {
                    Interlocked.Increment(ref self.CacheHits);
                }
                else
                {
                    Interlocked.Increment(ref self.CacheMisses);
                }
            }
            else
            {
                Interlocked.Increment(ref self.FailedOperations);
            }
            
            // 记录连接等待时间
            var waitTime = Environment.TickCount64 - startTime;
            Interlocked.Add(ref self.TotalConnectionWaitTime, waitTime);
        }
        
        /// <summary>
        /// 获取操作成功率
        /// </summary>
        public static float GetSuccessRate(this RedisComponent self)
        {
            long total = Volatile.Read(ref self.TotalOperations);
            long failed = Volatile.Read(ref self.FailedOperations);
            
            return total > 0 ? 
                (total - failed) * 100f / total : 100f;
        }
        
        /// <summary>
        /// 获取缓存命中率
        /// </summary>
        public static float GetCacheHitRate(this RedisComponent self)
        {
            long hits = Volatile.Read(ref self.CacheHits);
            long misses = Volatile.Read(ref self.CacheMisses);
            long total = hits + misses;
            
            return total > 0 ? hits * 100f / total : 0f;
        }
        
        /// <summary>
        /// 获取平均操作时间
        /// </summary>
        public static float GetAverageOperationTime(this RedisComponent self)
        {
            long total = Volatile.Read(ref self.TotalOperations);
            long totalTime = Volatile.Read(ref self.TotalOperationTime);
            
            return total > 0 ? totalTime * 1.0f / total : 0f;
        }
        
        /// <summary>
        /// 获取平均连接等待时间
        /// </summary>
        public static float GetAverageWaitTime(this RedisComponent self)
        {
            long total = Volatile.Read(ref self.TotalOperations);
            long totalWait = Volatile.Read(ref self.TotalConnectionWaitTime);
            
            return total > 0 ? totalWait * 1.0f / total : 0f;
        }
        
        /// <summary>
        /// 获取状态信息
        /// </summary>
        public static string GetStatus(this RedisComponent self)
        {
            if (self.ConnectionPool == null)
                return "Redis连接池: 未初始化";
                
            var poolStatus = self.ConnectionPool.GetStatus();
            var successRate = self.GetSuccessRate();
            var hitRate = self.GetCacheHitRate();
            var avgOpTime = self.GetAverageOperationTime();
            var avgWaitTime = self.GetAverageWaitTime();
            
            return $"{poolStatus}\n" +
                   $"操作统计: 总数={self.TotalOperations}, 成功={self.SuccessfulOperations}, 失败={self.FailedOperations}\n" +
                   $"缓存统计: 命中={self.CacheHits}, 未命中={self.CacheMisses}, 命中率={hitRate:F2}%\n" +
                   $"性能: 成功率={successRate:F2}%, 平均耗时={avgOpTime:F2}ms, 平均等待={avgWaitTime:F2}ms";
        }
        
        /// <summary>
        /// 记录统计信息
        /// </summary>
        public static void LogStatistics(this RedisComponent self, string context = "")
        {
            if (string.IsNullOrEmpty(context))
            {
                context = "定期统计";
            }
            
            Log.Info($"Redis统计[{context}]: {self.GetStatus()}");
        }
        
        /// <summary>
        /// 收集指标
        /// </summary>
        public static async ETTask CollectMetricsAsync(this RedisComponent self)
        {
            IDatabase db = null;
            try
            {
                db = self.ConnectionPool?.GetConnection();
                if (db == null) return;
                
                var server = self.ConnectionPool.GetServer();
                if (server == null) return;
                
                var info = await server.InfoAsync("all");
                
                // 使用 LINQ 查找指标
                var memoryUsed = info
                    .Where(s => s.Key == "Memory")
                    .SelectMany(s => s)
                    .FirstOrDefault(i => i.Key == "used_memory_human")
                    .Value;
                
                var connectedClients = info
                    .Where(s => s.Key == "Clients")
                    .SelectMany(s => s)
                    .FirstOrDefault(i => i.Key == "connected_clients")
                    .Value;
                
                var keyspaceHits = info
                    .Where(s => s.Key == "Stats")
                    .SelectMany(s => s)
                    .FirstOrDefault(i => i.Key == "keyspace_hits")
                    .Value;
                
                var keyspaceMisses = info
                    .Where(s => s.Key == "Stats")
                    .SelectMany(s => s)
                    .FirstOrDefault(i => i.Key == "keyspace_misses")
                    .Value;
                
                // 计算命中率
                if (long.TryParse(keyspaceHits, out var hits) && 
                    long.TryParse(keyspaceMisses, out var misses))
                {
                    var hitRate = hits + misses > 0 ? 
                        (hits * 100.0 / (hits + misses)).ToString("F2") + "%" : "0%";
                    
                    Log.Debug($"Redis指标: 内存={memoryUsed ?? "N/A"}, 连接={connectedClients ?? "N/A"}, 命中率={hitRate}");
                }
                else
                {
                    Log.Debug($"Redis指标: 内存={memoryUsed ?? "N/A"}, 连接={connectedClients ?? "N/A"}, 命中率=N/A");
                }
            }
            catch (Exception ex)
            {
                Log.Debug($"收集Redis指标失败: {ex.Message}");
            }
            finally
            {
                if (db != null)
                {
                    self.ConnectionPool.ReleaseConnection(db);
                }
            }
        }
        
        /// <summary>
        /// 重置统计信息
        /// </summary>
        public static void ResetStatistics(this RedisComponent self)
        {
            Interlocked.Exchange(ref self.TotalOperations, 0);
            Interlocked.Exchange(ref self.FailedOperations, 0);
            Interlocked.Exchange(ref self.SuccessfulOperations, 0);
            Interlocked.Exchange(ref self.TotalOperationTime, 0);
            Interlocked.Exchange(ref self.TotalConnectionWaitTime, 0);
            Interlocked.Exchange(ref self.CacheHits, 0);
            Interlocked.Exchange(ref self.CacheMisses, 0);
            Interlocked.Exchange(ref self.CacheEvictions, 0);
            
            Log.Info("Redis统计信息已重置");
        }
        
        // ========== 基本操作方法 ==========
        
        /// <summary>
        /// 执行Redis操作（带重试）
        /// </summary>
        public static async ETTask<T> ExecuteWithRetryAsync<T>(this RedisComponent self, 
            Func<IDatabase, Task<T>> operation, 
            int? maxRetries = null,
            bool isCacheOperation = false)
        {
            maxRetries ??= self.MaxRetryCount;
            var opInfo = self.BeginOperation();
            var retryCount = 0;
            
            while (retryCount < maxRetries)
            {
                IDatabase db = null;
                try
                {
                    db = self.ConnectionPool.GetConnection();
                    var result = await operation(db);
                    
                    self.EndOperation(opInfo, true, isCacheOperation);
                    return result;
                }
                catch (RedisException ex) when (IsConnectionException(ex))
                {
                    self.EndOperation(opInfo, false, false);
                    
                    if (retryCount < maxRetries - 1)
                    {
                        retryCount++;
                        Log.Warning($"Redis操作重试[{retryCount}/{maxRetries}]: {ex.Message}");
                        
                        SafeRelease(self, db);
                        await self.Scene().GetComponent<TimerComponent>().WaitAsync(self.RetryDelayMs * retryCount);
                        continue;
                    }
                    
                    Log.Error($"Redis操作失败，已达到最大重试次数: {ex}");
                    self.LastErrorMessage = ex.Message;
                    self.LastErrorTime = DateTime.Now;
                    throw new Exception($"Redis操作失败: {ex.Message}", ex);
                }
                catch (Exception ex)
                {
                    self.EndOperation(opInfo, false, false);
                    self.LastErrorMessage = ex.Message;
                    self.LastErrorTime = DateTime.Now;
                    
                    SafeRelease(self, db);
                    throw;
                }
                finally
                {
                    SafeRelease(self, db);
                }
            }
            
            throw new TimeoutException($"Redis操作失败，重试{maxRetries}次后仍然失败");
        }
        
        // ========== String操作 ==========
        
        public static async ETTask<bool> StringSetAsync(this RedisComponent self, 
            string key, string value, TimeSpan? expiry = null)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                if (expiry.HasValue)
                {
                    return await db.StringSetAsync(key, value, expiry.Value);
                }
                else
                {
                    return await db.StringSetAsync(key, value);
                }
            }, isCacheOperation: true);
        }
        
        public static async ETTask<bool> StringSetAsync<T>(this RedisComponent self,
            string key, T value, TimeSpan? expiry = null) where T : class
        {
            var json = JsonSerializer.Serialize(value);
            return await self.StringSetAsync(key, json, expiry);
        }
        
        public static async ETTask<string> StringGetAsync(this RedisComponent self, string key)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var value = await db.StringGetAsync(key);
                return value.HasValue ? value.ToString() : null;
            }, isCacheOperation: true);
        }
        
        public static async ETTask<T> StringGetAsync<T>(this RedisComponent self, string key) where T : class
        {
            var json = await self.StringGetAsync(key);
            if (string.IsNullOrEmpty(json))
                return default;
            
            try
            {
                return JsonSerializer.Deserialize<T>(json);
            }
            catch (Exception ex)
            {
                Log.Error($"反序列化Redis值失败: {ex.Message}");
                return default;
            }
        }
        
        public static async ETTask<long> StringIncrementAsync(this RedisComponent self, string key, long value = 1)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.StringIncrementAsync(key, value);
            });
        }
        
        public static async ETTask<long> StringDecrementAsync(this RedisComponent self, string key, long value = 1)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.StringDecrementAsync(key, value);
            });
        }
        
        // ========== Hash操作 ==========
        
        public static async ETTask<bool> HashSetAsync(this RedisComponent self, 
            string key, string field, string value)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.HashSetAsync(key, field, value);
            });
        }
        
        public static async ETTask<string> HashGetAsync(this RedisComponent self, 
            string key, string field)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var value = await db.HashGetAsync(key, field);
                return value.HasValue ? value.ToString() : null;
            });
        }
        
        public static async ETTask<Dictionary<string, string>> HashGetAllAsync(this RedisComponent self, string key)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var entries = await db.HashGetAllAsync(key);
                var result = new Dictionary<string, string>();
                
                foreach (var entry in entries)
                {
                    result[entry.Name.ToString()] = entry.Value.ToString();
                }
                
                return result;
            });
        }
        
        // ========== List操作 ==========
        
        public static async ETTask<long> ListLeftPushAsync(this RedisComponent self, 
            string key, params string[] values)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var redisValues = values.Select(v => (RedisValue)v).ToArray();
                return await db.ListLeftPushAsync(key, redisValues);
            });
        }
        
        public static async ETTask<string> ListLeftPopAsync(this RedisComponent self, string key)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var value = await db.ListLeftPopAsync(key);
                return value.HasValue ? value.ToString() : null;
            });
        }
        
        public static async ETTask<List<string>> ListRangeAsync(this RedisComponent self, 
            string key, long start = 0, long stop = -1)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var values = await db.ListRangeAsync(key, start, stop);
                return values.Select(v => v.ToString()).ToList();
            });
        }
        
        // ========== Set操作 ==========
        
        public static async ETTask<bool> SetAddAsync(this RedisComponent self, 
            string key, string value)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.SetAddAsync(key, value);
            });
        }
        
        public static async ETTask<bool> SetRemoveAsync(this RedisComponent self, 
            string key, string value)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.SetRemoveAsync(key, value);
            });
        }
        
        public static async ETTask<bool> SetContainsAsync(this RedisComponent self, 
            string key, string value)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.SetContainsAsync(key, value);
            });
        }
        
        // ========== SortedSet操作 ==========
        
        public static async ETTask<bool> SortedSetAddAsync(this RedisComponent self,
            string key, string member, double score)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.SortedSetAddAsync(key, member, score);
            });
        }
        
        public static async ETTask<List<string>> SortedSetRangeByScoreAsync(this RedisComponent self,
            string key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var values = await db.SortedSetRangeByScoreAsync(key, start, stop);
                return values.Select(v => v.ToString()).ToList();
            });
        }
        
        // ========== Key操作 ==========
        
        public static async ETTask<bool> KeyExistsAsync(this RedisComponent self, string key)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.KeyExistsAsync(key);
            }, isCacheOperation: true);
        }
        
        public static async ETTask<bool> KeyDeleteAsync(this RedisComponent self, string key)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var result = await db.KeyDeleteAsync(key);
                if (result)
                {
                    Interlocked.Increment(ref self.CacheEvictions);
                }
                return result;
            });
        }
        
        public static async ETTask<bool> KeyExpireAsync(this RedisComponent self, 
            string key, TimeSpan? expiry)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.KeyExpireAsync(key, expiry);
            });
        }
        
        public static async ETTask<TimeSpan?> KeyTimeToLiveAsync(this RedisComponent self, string key)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                return await db.KeyTimeToLiveAsync(key);
            });
        }
        
        // ========== 批量操作 ==========
        
        public static async ETTask<bool> StringSetMultipleAsync(this RedisComponent self,
            Dictionary<string, string> keyValues, TimeSpan? expiry = null)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var tasks = keyValues.Select(kv => 
                {
                    if (expiry.HasValue)
                    {
                        return db.StringSetAsync(kv.Key, kv.Value, expiry.Value);
                    }
                    else
                    {
                        return db.StringSetAsync(kv.Key, kv.Value);
                    }
                });
                
                var results = await Task.WhenAll(tasks);
                return results.All(r => r);
            });
        }
        
        public static async ETTask<Dictionary<string, string>> StringGetMultipleAsync(
            this RedisComponent self, params string[] keys)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var redisKeys = keys.Select(k => (RedisKey)k).ToArray();
                var values = await db.StringGetAsync(redisKeys);
                
                var result = new Dictionary<string, string>();
                for (int i = 0; i < keys.Length; i++)
                {
                    if (values[i].HasValue)
                    {
                        result[keys[i]] = values[i].ToString();
                    }
                }
                
                return result;
            }, isCacheOperation: true);
        }
        
        // ========== 分布式锁 ==========
        
        public static async ETTask<IDisposable> AcquireLockAsync(this RedisComponent self,
            string lockKey, 
            int? timeoutMs = null,
            int? retryCount = null,
            int? retryDelayMs = null)
        {
            timeoutMs ??= self.DefaultLockTimeout;
            retryCount ??= self.DefaultLockRetryCount;
            retryDelayMs ??= self.DefaultLockRetryDelay;
            
            var lockValue = Guid.NewGuid().ToString();
            var lockTimeout = TimeSpan.FromMilliseconds(timeoutMs.Value);
            
            for (int i = 0; i < retryCount.Value; i++)
            {
                try
                {
                    var acquired = await self.ExecuteWithRetryAsync(async db =>
                    {
                        return await db.LockTakeAsync(lockKey, lockValue, lockTimeout);
                    });
                    
                    if (acquired)
                    {
                        return new RedisLock(self, lockKey, lockValue);
                    }
                }
                catch (Exception ex)
                {
                    Log.Warning($"获取锁失败[{i+1}/{retryCount}]: {ex.Message}");
                }
                
                if (i < retryCount.Value - 1)
                {
                    await self.Scene().GetComponent<TimerComponent>().WaitAsync(retryDelayMs.Value);
                }
            }
            
            throw new TimeoutException($"获取分布式锁超时: {lockKey}");
        }
        
        // Redis锁内部类
        [EnableClass]
        private class RedisLock : IDisposable
        {
            private readonly RedisComponent _redis;
            private readonly string _lockKey;
            private readonly string _lockValue;
            private bool _disposed;
            
            public RedisLock(RedisComponent redis, string lockKey, string lockValue)
            {
                _redis = redis;
                _lockKey = lockKey;
                _lockValue = lockValue;
            }
            
            public void Dispose()
            {
                if (_disposed) return;
                _disposed = true;
                
                try
                {
                    // 异步释放但不等待
                    _ = ReleaseLockAsync();
                }
                catch (Exception ex)
                {
                    Log.Error($"启动Redis锁释放失败: {ex.Message}");
                }
            }
            
            private async ETTask ReleaseLockAsync()
            {
                try
                {
                    await _redis.ExecuteWithRetryAsync(async db =>
                    {
                        return await db.LockReleaseAsync(_lockKey, _lockValue);
                    });
                }
                catch (Exception ex)
                {
                    Log.Error($"释放Redis锁失败: {ex.Message}");
                }
            }
        }
        
        // ========== 发布订阅 ==========
        
        public static async ETTask SubscribeAsync(this RedisComponent self,
            string channel, Action<string, string> handler)
        {
            await self.ExecuteWithRetryAsync(async db =>
            {
                var subscriber = db.Multiplexer.GetSubscriber();
                var redisChannel = RedisChannel.Literal(channel);
                
                await subscriber.SubscribeAsync(redisChannel, (ch, message) =>
                {
                    handler(ch.ToString(), message.ToString());
                });
                
                return true;
            });
        }
        
        public static async ETTask SubscribePatternAsync(this RedisComponent self,
            string pattern, Action<string, string> handler)
        {
            await self.ExecuteWithRetryAsync(async db =>
            {
                var subscriber = db.Multiplexer.GetSubscriber();
                var redisChannel = RedisChannel.Pattern(pattern);
                
                await subscriber.SubscribeAsync(redisChannel, (ch, message) =>
                {
                    handler(ch.ToString(), message.ToString());
                });
                
                return true;
            });
        }
        
        public static async ETTask<long> PublishAsync(this RedisComponent self,
            string channel, string message)
        {
            return await self.ExecuteWithRetryAsync(async db =>
            {
                var subscriber = db.Multiplexer.GetSubscriber();
                var redisChannel = RedisChannel.Literal(channel);
                
                return await subscriber.PublishAsync(redisChannel, message);
            });
        }
        
        public static async ETTask UnsubscribeAsync(this RedisComponent self,
            string channel, bool patternMode = false)
        {
            await self.ExecuteWithRetryAsync(async db =>
            {
                var subscriber = db.Multiplexer.GetSubscriber();
                RedisChannel redisChannel = patternMode ? 
                    RedisChannel.Pattern(channel) : 
                    RedisChannel.Literal(channel);
                
                await subscriber.UnsubscribeAsync(redisChannel);
                return true;
            });
        }
        
        // ========== 工具方法 ==========
        
        public static async ETTask<Dictionary<string, string>> GetServerInfoAsync(this RedisComponent self)
        {
            try
            {
                var server = self.ConnectionPool.GetServer();
                if (server == null)
                    return new Dictionary<string, string>();
                
                var info = await server.InfoAsync("all");
                var result = new Dictionary<string, string>();
                
                foreach (var section in info)
                {
                    string sectionName = section.Key;
                    foreach (var item in section)
                    {
                        result[$"{sectionName}.{item.Key}"] = item.Value;
                    }
                }
                
                return result;
            }
            catch (Exception ex)
            {
                Log.Error($"获取Redis服务器信息失败: {ex.Message}");
                return new Dictionary<string, string>();
            }
        }
        
        public static async ETTask ForceReconnectAsync(this RedisComponent self)
        {
            Log.Info("强制重连Redis连接池...");
            
            Interlocked.Increment(ref self.TotalReconnects);
            
            if (self.ConnectionPool != null)
            {
                await self.ConnectionPool.ForceReconnectAllAsync();
            }
            
            Log.Info($"强制重连完成，总重连次数: {self.TotalReconnects}");
        }
        
        // ========== 私有辅助方法 ==========
        
        private static async ETTask WarmupPool(RedisComponent self)
        {
            Log.Info("预热Redis连接池...");
            
            if (self.ConnectionPool != null)
            {
                await self.ConnectionPool.WarmupAsync(self.MinPoolSize);
            }
            
            Log.Info($"预热完成: {self.MinPoolSize}个连接");
        }
        
        private static async ETTask ValidateConnection(RedisComponent self)
        {
            try
            {
                var db = self.ConnectionPool.GetConnection();
                var result = await db.PingAsync();
                self.ConnectionPool.ReleaseConnection(db);
                
                Log.Info($"Redis连接验证成功，延迟: {result.TotalMilliseconds:F2}ms");
            }
            catch (Exception ex)
            {
                Log.Error($"Redis连接验证失败: {ex.Message}");
                throw;
            }
        }
        
        private static async ETTask InitializePubSub(RedisComponent self)
        {
            try
            {
                foreach (var channel in self.SubscribeChannels)
                {
                    await self.SubscribeAsync(channel, (ch, msg) =>
                    {
                        Log.Debug($"收到Redis消息 [{ch}]: {msg}");
                    });
                }
                
                Log.Info($"Redis发布订阅初始化完成，订阅频道: {string.Join(", ", self.SubscribeChannels)}");
            }
            catch (Exception ex)
            {
                Log.Error($"Redis发布订阅初始化失败: {ex.Message}");
            }
        }
        
        private static bool IsConnectionException(RedisException ex)
        {
            if (ex == null) return false;
            
            var message = ex.Message.ToLowerInvariant();
            return message.Contains("connection") ||
                   message.Contains("timeout") ||
                   message.Contains("socket") ||
                   message.Contains("network") ||
                   message.Contains("无法连接") ||
                   message.Contains("超时");
        }
        
        private static void SafeRelease(RedisComponent self, IDatabase db)
        {
            try
            {
                if (db != null)
                {
                    self.ConnectionPool.ReleaseConnection(db);
                }
            }
            catch (Exception ex)
            {
                Log.Debug($"释放Redis连接异常: {ex.Message}");
            }
        }
    }
}