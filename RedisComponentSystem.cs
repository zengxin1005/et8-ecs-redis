/*
 * 文件名: RedisComponentSystem.cs
 * 作者: zengxin
 * 创建日期: 2026-2-25
 */

using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace ET.Server
{
    [EntitySystemOf(typeof(RedisComponent))]
    [FriendOf(typeof(RedisComponent))]
    public static partial class RedisComponentSystem
    {
        #region 生命周期
        
        [EntitySystem]
        private static void Awake(this RedisComponent self, string connectionString, int databaseNumber)
        {
            self.ConnectionString = connectionString;
            self.DatabaseNumber = databaseNumber;
            
            try
            {
                var config = ConfigurationOptions.Parse(connectionString);
                config.AbortOnConnectFail = false;
                config.ConnectTimeout = 5000;
                config.SyncTimeout = 5000;
                
                self.Multiplexer = ConnectionMultiplexer.Connect(config);
                
                // 测试连接
                var db = self.Database;
                db.Ping();
                
                Log.Info($"RedisComponent 初始化成功: {connectionString}, DB:{databaseNumber}");
            }
            catch (Exception ex)
            {
                Log.Error($"RedisComponent 初始化失败: {ex.Message}");
            }
        }
        
        [EntitySystem]
        private static void Destroy(this RedisComponent self)
        {
            try
            {
                self.Multiplexer?.Close();
                self.Multiplexer?.Dispose();
                Log.Info($"RedisComponent 已销毁");
            }
            catch (Exception ex)
            {
                Log.Warning($"RedisComponent 销毁时出错: {ex.Message}");
            }
        }
        
        #endregion
        
        #region String 类型操作
        
        /// <summary>
        /// 设置字符串值 - Expiration 类型
        /// </summary>
        public static async ETTask<bool> StringSet(this RedisComponent self, string key, string value, TimeSpan? expiry = null)
        {
            try
            {
                if (expiry.HasValue)
                {
                    // 有过期时间
                    return await self.Database.StringSetAsync(key, value, expiry.Value);
                }
                else
                {
                    // 无过期时间
                    return await self.Database.StringSetAsync(key, value);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Redis StringSet 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 设置字符串值（泛型）
        /// </summary>
        public static async ETTask<bool> StringSet<T>(this RedisComponent self, string key, T value, TimeSpan? expiry = null)
        {
            try
            {
                var json = MongoHelper.ToJson(value);
        
                if (expiry.HasValue)
                {
                    return await self.Database.StringSetAsync(key, json, expiry.Value);
                }
                else
                {
                    return await self.Database.StringSetAsync(key, json);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"Redis StringSet<T> 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 获取字符串值
        /// </summary>
        public static async ETTask<string> StringGet(this RedisComponent self, string key)
        {
            try
            {
                var value = await self.Database.StringGetAsync(key);
                return value.HasValue ? value.ToString() : null;
            }
            catch (Exception ex)
            {
                Log.Error($"Redis StringGet 失败: {key}, {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// 获取字符串值（泛型）
        /// </summary>
        public static async ETTask<T> StringGet<T>(this RedisComponent self, string key) where T : class
        {
            try
            {
                var value = await self.Database.StringGetAsync(key);
                if (!value.HasValue) return null;
                
                return MongoHelper.FromJson<T>(value.ToString());
            }
            catch (Exception ex)
            {
                Log.Error($"Redis StringGet<T> 失败: {key}, {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// 原子递增
        /// </summary>
        public static async ETTask<long> StringIncrement(this RedisComponent self, string key, long value = 1)
        {
            try
            {
                return await self.Database.StringIncrementAsync(key, value);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis StringIncrement 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        /// <summary>
        /// 原子递减
        /// </summary>
        public static async ETTask<long> StringDecrement(this RedisComponent self, string key, long value = 1)
        {
            try
            {
                return await self.Database.StringDecrementAsync(key, value);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis StringDecrement 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        #endregion
        
        #region Hash 类型操作
        
        /// <summary>
        /// 设置Hash字段
        /// </summary>
        public static async ETTask<bool> HashSet(this RedisComponent self, string key, string field, string value)
        {
            try
            {
                await self.Database.HashSetAsync(key, field, value);
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"Redis HashSet 失败: {key}:{field}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 设置Hash字段（泛型）
        /// </summary>
        public static async ETTask<bool> HashSet<T>(this RedisComponent self, string key, string field, T value)
        {
            try
            {
                var json = MongoHelper.ToJson(value);
                await self.Database.HashSetAsync(key, field, json);
                return true;
            }
            catch (Exception ex)
            {
                Log.Error($"Redis HashSet<T> 失败: {key}:{field}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 批量设置Hash字段
        /// </summary>
        public static async ETTask HashSetBatch(this RedisComponent self, string key, IEnumerable<KeyValuePair<string, string>> pairs)
        {
            try
            {
                var entries = pairs.Select(p => new HashEntry(p.Key, p.Value)).ToArray();
                await self.Database.HashSetAsync(key, entries);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis HashSetBatch 失败: {key}, {ex.Message}");
            }
        }
        
        /// <summary>
        /// 获取Hash字段
        /// </summary>
        public static async ETTask<string> HashGet(this RedisComponent self, string key, string field)
        {
            try
            {
                var value = await self.Database.HashGetAsync(key, field);
                return value.HasValue ? value.ToString() : null;
            }
            catch (Exception ex)
            {
                Log.Error($"Redis HashGet 失败: {key}:{field}, {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// 获取Hash字段（泛型）
        /// </summary>
        public static async ETTask<T> HashGet<T>(this RedisComponent self, string key, string field) where T : class
        {
            try
            {
                var value = await self.Database.HashGetAsync(key, field);
                if (!value.HasValue) return null;
                
                return MongoHelper.FromJson<T>(value.ToString());
            }
            catch (Exception ex)
            {
                Log.Error($"Redis HashGet<T> 失败: {key}:{field}, {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// 获取所有Hash字段
        /// </summary>
        public static async ETTask<Dictionary<string, string>> HashGetAll(this RedisComponent self, string key)
        {
            try
            {
                var entries = await self.Database.HashGetAllAsync(key);
                return entries.ToDictionary(
                    e => e.Name.ToString(),
                    e => e.Value.ToString()
                );
            }
            catch (Exception ex)
            {
                Log.Error($"Redis HashGetAll 失败: {key}, {ex.Message}");
                return new Dictionary<string, string>();
            }
        }
        
        /// <summary>
        /// 删除Hash字段
        /// </summary>
        public static async ETTask<bool> HashDelete(this RedisComponent self, string key, string field)
        {
            try
            {
                return await self.Database.HashDeleteAsync(key, field);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis HashDelete 失败: {key}:{field}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 判断Hash字段是否存在
        /// </summary>
        public static async ETTask<bool> HashExists(this RedisComponent self, string key, string field)
        {
            try
            {
                return await self.Database.HashExistsAsync(key, field);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis HashExists 失败: {key}:{field}, {ex.Message}");
                return false;
            }
        }
        
        #endregion
        
        #region List 类型操作
        
        /// <summary>
        /// 列表左侧推入
        /// </summary>
        public static async ETTask<long> ListLeftPush(this RedisComponent self, string key, string value)
        {
            try
            {
                return await self.Database.ListLeftPushAsync(key, value);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis ListLeftPush 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        /// <summary>
        /// 列表右侧推入
        /// </summary>
        public static async ETTask<long> ListRightPush(this RedisComponent self, string key, string value)
        {
            try
            {
                return await self.Database.ListRightPushAsync(key, value);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis ListRightPush 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        /// <summary>
        /// 列表左侧弹出
        /// </summary>
        public static async ETTask<string> ListLeftPop(this RedisComponent self, string key)
        {
            try
            {
                var value = await self.Database.ListLeftPopAsync(key);
                return value.HasValue ? value.ToString() : null;
            }
            catch (Exception ex)
            {
                Log.Error($"Redis ListLeftPop 失败: {key}, {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// 列表右侧弹出
        /// </summary>
        public static async ETTask<string> ListRightPop(this RedisComponent self, string key)
        {
            try
            {
                var value = await self.Database.ListRightPopAsync(key);
                return value.HasValue ? value.ToString() : null;
            }
            catch (Exception ex)
            {
                Log.Error($"Redis ListRightPop 失败: {key}, {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// 获取列表范围
        /// </summary>
        public static async ETTask<List<string>> ListRange(this RedisComponent self, string key, long start = 0, long stop = -1)
        {
            try
            {
                var values = await self.Database.ListRangeAsync(key, start, stop);
                return values.Select(v => v.ToString()).ToList();
            }
            catch (Exception ex)
            {
                Log.Error($"Redis ListRange 失败: {key}, {ex.Message}");
                return new List<string>();
            }
        }
        
        /// <summary>
        /// 获取列表长度
        /// </summary>
        public static async ETTask<long> ListLength(this RedisComponent self, string key)
        {
            try
            {
                return await self.Database.ListLengthAsync(key);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis ListLength 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        #endregion
        
        #region Set 类型操作
        
        /// <summary>
        /// 集合添加
        /// </summary>
        public static async ETTask<bool> SetAdd(this RedisComponent self, string key, string value)
        {
            try
            {
                return await self.Database.SetAddAsync(key, value);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SetAdd 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 批量集合添加
        /// </summary>
        public static async ETTask<long> SetAddBatch(this RedisComponent self, string key, IEnumerable<string> values)
        {
            try
            {
                var members = values.Select(v => (RedisValue)v).ToArray();
                return await self.Database.SetAddAsync(key, members);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SetAddBatch 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        /// <summary>
        /// 集合移除
        /// </summary>
        public static async ETTask<bool> SetRemove(this RedisComponent self, string key, string value)
        {
            try
            {
                return await self.Database.SetRemoveAsync(key, value);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SetRemove 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 获取所有集合成员
        /// </summary>
        public static async ETTask<List<string>> SetMembers(this RedisComponent self, string key)
        {
            try
            {
                var members = await self.Database.SetMembersAsync(key);
                return members.Select(m => m.ToString()).ToList();
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SetMembers 失败: {key}, {ex.Message}");
                return new List<string>();
            }
        }
        
        /// <summary>
        /// 判断是否是集合成员
        /// </summary>
        public static async ETTask<bool> SetContains(this RedisComponent self, string key, string value)
        {
            try
            {
                return await self.Database.SetContainsAsync(key, value);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SetContains 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 获取集合长度
        /// </summary>
        public static async ETTask<long> SetLength(this RedisComponent self, string key)
        {
            try
            {
                return await self.Database.SetLengthAsync(key);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SetLength 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        #endregion
        
        #region SortedSet 类型操作（排行榜）
        
        /// <summary>
        /// 有序集合添加
        /// </summary>
        public static async ETTask<bool> SortedSetAdd(this RedisComponent self, string key, string member, double score)
        {
            try
            {
                return await self.Database.SortedSetAddAsync(key, member, score);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SortedSetAdd 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 批量有序集合添加
        /// </summary>
        public static async ETTask<long> SortedSetAddBatch(this RedisComponent self, string key, IEnumerable<(string member, double score)> items)
        {
            try
            {
                var entries = items.Select(i => new SortedSetEntry(i.member, i.score)).ToArray();
                return await self.Database.SortedSetAddAsync(key, entries);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SortedSetAddBatch 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        /// <summary>
        /// 获取排行榜（从高到低）
        /// </summary>
        public static async ETTask<List<(string member, double score)>> SortedSetRangeByRank(this RedisComponent self, 
            string key, 
            long start = 0, 
            long stop = -1, 
            bool descending = true)
        {
            try
            {
                var order = descending ? Order.Descending : Order.Ascending;
                var entries = await self.Database.SortedSetRangeByRankWithScoresAsync(key, start, stop, order);
                return entries.Select(e => (e.Element.ToString(), e.Score)).ToList();
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SortedSetRangeByRank 失败: {key}, {ex.Message}");
                return new List<(string, double)>();
            }
        }
        
        /// <summary>
        /// 获取成员分数
        /// </summary>
        public static async ETTask<double?> SortedSetScore(this RedisComponent self, string key, string member)
        {
            try
            {
                var score = await self.Database.SortedSetScoreAsync(key, member);
                return score.HasValue ? (double?)score.Value : null;
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SortedSetScore 失败: {key}, {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// 获取成员排名（从高到低，0开始）
        /// </summary>
        public static async ETTask<long?> SortedSetRank(this RedisComponent self, string key, string member, bool descending = true)
        {
            try
            {
                var order = descending ? Order.Descending : Order.Ascending;
                return await self.Database.SortedSetRankAsync(key, member, order);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SortedSetRank 失败: {key}, {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// 增加成员分数
        /// </summary>
        public static async ETTask<double> SortedSetIncrement(this RedisComponent self, string key, string member, double value)
        {
            try
            {
                return await self.Database.SortedSetIncrementAsync(key, member, value);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SortedSetIncrement 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        /// <summary>
        /// 获取有序集合长度
        /// </summary>
        public static async ETTask<long> SortedSetLength(this RedisComponent self, string key)
        {
            try
            {
                return await self.Database.SortedSetLengthAsync(key);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis SortedSetLength 失败: {key}, {ex.Message}");
                return 0;
            }
        }
        
        #endregion
        
        #region Key 操作
        
        /// <summary>
        /// 删除键
        /// </summary>
        public static async ETTask<bool> KeyDelete(this RedisComponent self, string key)
        {
            try
            {
                return await self.Database.KeyDeleteAsync(key);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis KeyDelete 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 批量删除键
        /// </summary>
        public static async ETTask<long> KeyDeleteBatch(this RedisComponent self, IEnumerable<string> keys)
        {
            try
            {
                var redisKeys = keys.Select(k => (RedisKey)k).ToArray();
                return await self.Database.KeyDeleteAsync(redisKeys);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis KeyDeleteBatch 失败: {ex.Message}");
                return 0;
            }
        }
        
        /// <summary>
        /// 判断键是否存在
        /// </summary>
        public static async ETTask<bool> KeyExists(this RedisComponent self, string key)
        {
            try
            {
                return await self.Database.KeyExistsAsync(key);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis KeyExists 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 设置过期时间
        /// </summary>
        public static async ETTask<bool> KeyExpire(this RedisComponent self, string key, TimeSpan expiry)
        {
            try
            {
                return await self.Database.KeyExpireAsync(key, expiry);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis KeyExpire 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 设置过期时间（绝对时间）
        /// </summary>
        public static async ETTask<bool> KeyExpireAt(this RedisComponent self, string key, DateTime expiry)
        {
            try
            {
                return await self.Database.KeyExpireAsync(key, expiry);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis KeyExpireAt 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 获取剩余存活时间
        /// </summary>
        public static async ETTask<TimeSpan?> KeyTimeToLive(this RedisComponent self, string key)
        {
            try
            {
                return await self.Database.KeyTimeToLiveAsync(key);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis KeyTimeToLive 失败: {key}, {ex.Message}");
                return null;
            }
        }
        
        #endregion
        
        #region 分布式锁
        
        /// <summary>
        /// 获取分布式锁（使用SET NX EX）
        /// </summary>
        public static async ETTask<bool> LockTake(this RedisComponent self, string key, string value, TimeSpan expiry)
        {
            try
            {
                return await self.Database.LockTakeAsync(key, value, expiry);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis LockTake 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 释放分布式锁
        /// </summary>
        public static async ETTask<bool> LockRelease(this RedisComponent self, string key, string value)
        {
            try
            {
                return await self.Database.LockReleaseAsync(key, value);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis LockRelease 失败: {key}, {ex.Message}");
                return false;
            }
        }
        
        /// <summary>
        /// 执行分布式锁包裹的代码
        /// </summary>
        public static async ETTask<T> LockExecute<T>(this RedisComponent self, string key, TimeSpan expiry, Func<Task<T>> func)
        {
            var lockValue = Guid.NewGuid().ToString("N");
            
            if (!await self.LockTake(key, lockValue, expiry))
            {
                throw new Exception($"获取分布式锁失败: {key}");
            }
            
            try
            {
                return await func();
            }
            finally
            {
                await self.LockRelease(key, lockValue);
            }
        }
        
        /// <summary>
        /// 执行分布式锁包裹的代码（无返回值）
        /// </summary>
        public static async ETTask LockExecute(this RedisComponent self, string key, TimeSpan expiry, Func<Task> action)
        {
            var lockValue = Guid.NewGuid().ToString("N");
            
            if (!await self.LockTake(key, lockValue, expiry))
            {
                throw new Exception($"获取分布式锁失败: {key}");
            }
            
            try
            {
                await action();
            }
            finally
            {
                await self.LockRelease(key, lockValue);
            }
        }
        
        #endregion
        
        #region 发布订阅 -  RedisChannel 过时警告
        
        /// <summary>
        /// 发布消息
        /// </summary>
        public static async ETTask<long> Publish(this RedisComponent self, string channel, string message)
        {
            try
            {
                var redisChannel = RedisChannel.Literal(channel);
                return await self.Multiplexer.GetSubscriber().PublishAsync(redisChannel, message);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis Publish 失败: {channel}, {ex.Message}");
                return 0;
            }
        }
        
        /// <summary>
        /// 订阅消息
        /// </summary>
        public static void Subscribe(this RedisComponent self, string channel, Action<string> handler)
        {
            try
            {
                var redisChannel = RedisChannel.Literal(channel);
                self.Multiplexer.GetSubscriber().Subscribe(redisChannel, (_, message) =>
                {
                    handler(message.ToString());
                });
            }
            catch (Exception ex)
            {
                Log.Error($"Redis Subscribe 失败: {channel}, {ex.Message}");
            }
        }
        
        /// <summary>
        /// 取消订阅
        /// </summary>
        public static async ETTask Unsubscribe(this RedisComponent self, string channel)
        {
            try
            {
                var redisChannel = RedisChannel.Literal(channel);
                await self.Multiplexer.GetSubscriber().UnsubscribeAsync(redisChannel);
            }
            catch (Exception ex)
            {
                Log.Error($"Redis Unsubscribe 失败: {channel}, {ex.Message}");
            }
        }
        
        #endregion
        
        #region 执行Lua脚本

        /// <summary>
        /// 执行Lua脚本 - 
        /// </summary>
        public static async ETTask<RedisResult> ScriptEvaluate(this RedisComponent self, string script, object parameters = null)
        {
            try
            {
                var prepared = LuaScript.Prepare(script);
                var result = await self.Database.ScriptEvaluateAsync(prepared, parameters);
                return result; 
            }
            catch (Exception ex)
            {
                Log.Error($"Redis ScriptEvaluate 失败: {ex.Message}");
                return null; 
            }
        }

        #endregion

        #region 通用方法

        /// <summary>
        /// 执行自定义Redis命令 
        /// </summary>
        public static async ETTask<RedisResult> Execute(this RedisComponent self, string command, params object[] args)
        {
            try
            {
                var result = await self.Database.ExecuteAsync(command, args);
                return result;
            }
            catch (Exception ex)
            {
                Log.Error($"Redis Execute 失败: {command}, {ex.Message}");
                return null; 
            }
        }

        /// <summary>
        /// 获取服务器信息
        /// </summary>
        public static Dictionary<string, string> GetServerInfo(this RedisComponent self)
        {
            try
            {
                if (self.Server == null) return new Dictionary<string, string>();
        
                var info = self.Server.Info();
                var result = new Dictionary<string, string>();
                
                foreach (var group in info)
                {
                    var sectionName = group.Key;  // 分组名，如 "Server", "Clients" 等
                    var values = new List<string>();
                    
                    foreach (var kv in group)
                    {
                        values.Add($"{kv.Key}={kv.Value}");
                    }
            
                    result[sectionName] = string.Join(", ", values);
                }
        
                return result;
            }
            catch (Exception ex)
            {
                Log.Error($"Redis GetServerInfo 失败: {ex.Message}");
                return new Dictionary<string, string>();
            }
        }

        #endregion
    }
}