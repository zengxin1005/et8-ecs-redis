/*
 * 文件名: RedisExtensions.cs
 * 作者: zengxin
 * 创建日期: 2025-12-11
 */

using System;
using System.Collections.Generic;
using StackExchange.Redis;

namespace ET
{
    public static class RedisExtensions
    {
        /// <summary>
        /// 获取Redis组件
        /// </summary>
        public static RedisComponent Redis(this Scene scene)
        {
            return scene.GetComponent<RedisComponent>() ?? 
                   throw new Exception("RedisComponent未找到，请确保已添加RedisModule");
        }
        
        /// <summary>
        /// 设置缓存（带默认过期时间）
        /// </summary>
        public static async ETTask<bool> SetCacheAsync<T>(this RedisComponent redis,
            string key, T value, TimeSpan? expiry = null) where T : class
        {
            expiry ??= TimeSpan.FromMinutes(30);
            return await redis.StringSetAsync(key, value, expiry);
        }
        
        /// <summary>
        /// 获取缓存（带类型转换）
        /// </summary>
        public static async ETTask<T> GetCacheAsync<T>(this RedisComponent redis,
            string key) where T : class
        {
            return await redis.StringGetAsync<T>(key);
        }
        
        /// <summary>
        /// 获取或设置缓存
        /// </summary>
        public static async ETTask<T> GetOrSetAsync<T>(this RedisComponent redis,
            string key, Func<ETTask<T>> valueFactory, TimeSpan? expiry = null) where T : class
        {
            var value = await redis.GetCacheAsync<T>(key);
            if (value != null)
                return value;
            
            value = await valueFactory();
            if (value != null)
            {
                await redis.SetCacheAsync(key, value, expiry);
            }
            
            return value;
        }
        
        /// <summary>
        /// 批量删除缓存
        /// </summary>
        public static async ETTask<long> DeleteMultipleAsync(this RedisComponent redis,
            params string[] keys)
        {
            long deletedCount = 0;
            
            foreach (var key in keys)
            {
                if (await redis.KeyDeleteAsync(key))
                {
                    deletedCount++;
                }
            }
            
            return deletedCount;
        }
        
        /// <summary>
        /// 清空当前数据库
        /// </summary>
        public static async ETTask<bool> FlushDatabaseAsync(this RedisComponent redis)
        {
            try
            {
                var server = redis.ConnectionPool.GetServer();
                if (server == null)
                {
                    Log.Warning("Redis服务器不可用");
                    return false;
                }
                
                await server.FlushDatabaseAsync();
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"清空Redis数据库失败: {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 获取所有键
        /// </summary>
        public static async ETTask<List<string>> GetAllKeysAsync(this RedisComponent redis,
            string pattern = "*")
        {
            try
            {
                var server = redis.ConnectionPool.GetServer();
                if (server == null)
                {
                    return new List<string>();
                }
                
                // 使用 await foreach 遍历异步枚举
                var keys = new List<string>();
                await foreach (var redisKey in server.KeysAsync(pattern: pattern))
                {
                    keys.Add(redisKey.ToString());
                }
                
                return keys;
            }
            catch (Exception ex)
            {
                Log.Error($"获取Redis键列表失败: {ex.Message}");
                return new List<string>();
            }
        }
        
        /// <summary>
        /// 检查Redis是否健康
        /// </summary>
        public static async ETTask<bool> HealthCheckAsync(this RedisComponent redis)
        {
            try
            {
                var db = redis.ConnectionPool.GetConnection();
                var result = await db.PingAsync();
                redis.ConnectionPool.ReleaseConnection(db);
                
                return result.TotalMilliseconds < 1000; // 1秒内响应视为健康
            }
            catch
            {
                return false;
            }
        }
        
        /// <summary>
        /// 使用分布式锁执行操作
        /// </summary>
        public static async ETTask<T> ExecuteWithLockAsync<T>(this RedisComponent redis,
            string lockKey, Func<ETTask<T>> operation,
            int? timeoutMs = null, int? retryCount = null)
        {
            using (await redis.AcquireLockAsync(lockKey, timeoutMs, retryCount))
            {
                return await operation();
            }
        }
        
        /// <summary>
        /// 使用分布式锁执行操作（无返回值）
        /// </summary>
        public static async ETTask ExecuteWithLockAsync(this RedisComponent redis,
            string lockKey, Func<ETTask> operation,
            int? timeoutMs = null, int? retryCount = null)
        {
            using (await redis.AcquireLockAsync(lockKey, timeoutMs, retryCount))
            {
                await operation();
            }
        }
        
        /// <summary>
        /// 实现滑动窗口限流
        /// </summary>
        public static async ETTask<bool> RateLimitAsync(this RedisComponent redis,
            string key, int maxRequests, TimeSpan window)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var windowStart = now - (long)window.TotalMilliseconds;
            
            // 使用SortedSet实现滑动窗口
            await redis.SortedSetAddAsync(key, now.ToString(), now);
            
            // 移除窗口外的记录
            await redis.ExecuteWithRetryAsync(async db =>
            {
                return await db.SortedSetRemoveRangeByScoreAsync(key, 0, windowStart);
            });
            
            // 获取当前窗口内的请求数
            var count = await redis.ExecuteWithRetryAsync(async db =>
            {
                return await db.SortedSetLengthAsync(key);
            });
            
            // 设置键的过期时间
            await redis.KeyExpireAsync(key, window.Add(TimeSpan.FromSeconds(1)));
            
            return count <= maxRequests;
        }
        
        /// <summary>
        /// 实现简单的布隆过滤器（使用Redis的BitMap）
        /// </summary>
        public static async ETTask<bool> BloomFilterAddAsync(this RedisComponent redis,
            string filterKey, string value, int hashCount = 5)
        {
            var hashes = GetHashes(value, hashCount);
            
            foreach (var hash in hashes)
            {
                await redis.ExecuteWithRetryAsync(async db =>
                {
                    var result = await db.StringSetBitAsync(filterKey, hash, true);
                    return result == true;
                });
            }
            
            return true;
        }
        
        public static async ETTask<bool> BloomFilterExistsAsync(this RedisComponent redis,
            string filterKey, string value, int hashCount = 5)
        {
            var hashes = GetHashes(value, hashCount);
            
            foreach (var hash in hashes)
            {
                var exists = await redis.ExecuteWithRetryAsync(async db =>
                {
                    var result = await db.StringGetBitAsync(filterKey, hash);
                    return result == true;
                });
                
                if (!exists)
                {
                    return false;  // 如果任意一个位为0，说明一定不存在
                }
            }
            
            return true;  // 所有位都为1，可能存在（有误判率）
        }
        
        /// <summary>
        /// 异步获取键列表（扩展方法）
        /// </summary>
        public static async ETTask<List<string>> GetKeysAsync(this RedisComponent redis,
            int database = -1, RedisValue pattern = default, int pageSize = 10)
        {
            try
            {
                var server = redis.ConnectionPool.GetServer();
                if (server == null)
                    return new List<string>();
                
                // 使用正确的 KeysAsync 方法进行异步扫描
                var keysEnumerable = server.KeysAsync(
                    database: database,
                    pattern: pattern,
                    pageSize: pageSize
                );
                
                var keys = new List<string>();
                // 异步枚举所有返回的键
                await foreach (var redisKey in keysEnumerable)
                {
                    keys.Add(redisKey.ToString());
                }
                
                return keys;
            }
            catch (Exception ex)
            {
                Log.Error($"异步获取Redis键列表失败: {ex.Message}");
                return new List<string>();
            }
        }
        
        /// <summary>
        /// 检查键是否存在（批量）
        /// </summary>
        public static async ETTask<Dictionary<string, bool>> KeyExistsMultipleAsync(
            this RedisComponent redis, params string[] keys)
        {
            var results = new Dictionary<string, bool>();
            
            foreach (var key in keys)
            {
                var exists = await redis.KeyExistsAsync(key);
                results[key] = exists;
            }
            
            return results;
        }
        
        /// <summary>
        /// 设置多个键的过期时间
        /// </summary>
        public static async ETTask<Dictionary<string, bool>> KeyExpireMultipleAsync(
            this RedisComponent redis, Dictionary<string, TimeSpan> keyExpiries)
        {
            var results = new Dictionary<string, bool>();
            
            foreach (var kv in keyExpiries)
            {
                var success = await redis.KeyExpireAsync(kv.Key, kv.Value);
                results[kv.Key] = success;
            }
            
            return results;
        }
        
        /// <summary>
        /// 计算哈希值
        /// </summary>
        private static long[] GetHashes(string value, int count)
        {
            var hashes = new long[count];
            
            // 使用不同的哈希种子
            int hash1 = GetStableHash(value);
            int hash2 = GetStableHash(value + "salt"); // 添加盐值确保不同的哈希
            
            for (int i = 0; i < count; i++)
            {
                // 使用双重哈希技术
                hashes[i] = Math.Abs((long)(hash1 + i * hash2) % (1L << 32));
            }
            
            return hashes;
        }
        
        /// <summary>
        /// 获取稳定的哈希值
        /// </summary>
        private static int GetStableHash(string value)
        {
            unchecked
            {
                int hash = 23;
                foreach (char c in value)
                {
                    hash = hash * 31 + c;
                }
                return hash;
            }
        }
    }
}