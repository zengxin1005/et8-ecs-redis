/*
 * 文件名: RedisConnectionPool.cs
 * 作者: zengxin
 * 创建日期: 2025-12-11
 */


using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using StackExchange.Redis;
using System.Collections.Generic;
namespace ET
{
    /// <summary>
    /// 线程安全的Redis连接池
    /// </summary>
    [EnableClass]
    public class RedisConnectionPool : IDisposable
    {
        private readonly ConfigurationOptions _config;
        private ConnectionMultiplexer _multiplexer;
        private readonly ConcurrentQueue<IDatabase> _connections;
        private readonly SemaphoreSlim _semaphore;
        private int _createdCount;
        private readonly int _maxSize;
        private readonly int _minSize;
        private volatile bool _disposed;
        private long _totalReconnects;
        private readonly Scene _scene;
        
        // 线程安全的计数器（移除volatile，改为使用int类型）
        private int _availableCount;
        
        // 性能监控
        private long _totalGetTime;
        private long _totalGetCount;
        
        // 事件处理器
        public event EventHandler<RedisErrorEventArgs> Error;
        public event EventHandler<ConnectionFailedEventArgs> ConnectionFailed;
        public event EventHandler<InternalErrorEventArgs> InternalError;
        public event EventHandler<ConnectionFailedEventArgs> ConnectionRestored;
        
        // 统计信息
        public int ActiveConnections => Interlocked.CompareExchange(ref _createdCount, 0, 0);
        public int AvailableConnections => Interlocked.CompareExchange(ref _availableCount, 0, 0);
        public long TotalReconnects => Interlocked.Read(ref _totalReconnects);
        public bool IsConnected => _multiplexer?.IsConnected ?? false;
        
        // 健康状态
        public bool IsHealthy
        {
            get
            {
                if (_disposed) return false;
                if (_multiplexer == null) return false;
                return _multiplexer.IsConnected && ActiveConnections > 0;
            }
        }
        
        public RedisConnectionPool(Scene scene, string connectionString, int maxSize = 50, int minSize = 5)
        {
            if (scene == null) throw new ArgumentNullException(nameof(scene));
            
            _scene = scene;
            _maxSize = maxSize > 0 ? maxSize : 50;
            _minSize = Math.Min(minSize, _maxSize);
            
            _config = ConfigurationOptions.Parse(connectionString);
            _config.AbortOnConnectFail = false;
            _config.ConnectRetry = 5;
            _config.ReconnectRetryPolicy = new ExponentialRetry(5000);
            _config.KeepAlive = 60;
            _config.ConnectTimeout = 5000;
            _config.SyncTimeout = 5000;
            _config.AllowAdmin = true;
            
            _connections = new ConcurrentQueue<IDatabase>();
            _semaphore = new SemaphoreSlim(_maxSize, _maxSize);
            
            // 创建连接复用器
            _multiplexer = ConnectionMultiplexer.Connect(_config);
            SetupEventHandlers();
            
            Log.Info($"Redis连接池初始化完成，配置: 最小={_minSize}, 最大={_maxSize}");
        }
        
        private void SetupEventHandlers()
        {
            _multiplexer.ErrorMessage += (sender, args) =>
            {
                Error?.Invoke(this, args);
                Log.Error($"Redis错误: {args.Message}");
            };
            
            _multiplexer.ConnectionFailed += (sender, args) =>
            {
                ConnectionFailed?.Invoke(this, args);
                Log.Warning($"Redis连接失败: {args.FailureType}, {args.Exception?.Message}");
            };
            
            _multiplexer.InternalError += (sender, args) =>
            {
                InternalError?.Invoke(this, args);
                Log.Error($"Redis内部错误: {args.Exception}");
            };
            
            _multiplexer.ConnectionRestored += (sender, args) =>
            {
                // 修复：使用ConnectionFailedEventArgs
                ConnectionRestored?.Invoke(this, args);
                Log.Info($"Redis连接恢复: {args.FailureType}");
                Interlocked.Increment(ref _totalReconnects);
            };
        }
        
        /// <summary>
        /// 获取数据库连接
        /// </summary>
        public IDatabase GetConnection()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(RedisConnectionPool));
            
            var startTime = Environment.TickCount64;
            
            try
            {
                _semaphore.Wait();
                
                if (_disposed)
                    throw new ObjectDisposedException(nameof(RedisConnectionPool));
                
                if (_connections.TryDequeue(out var connection))
                {
                    Interlocked.Decrement(ref _availableCount);
                    return connection;
                }
                
                // 创建新连接
                return CreateNewConnection();
            }
            finally
            {
                var endTime = Environment.TickCount64;
                Interlocked.Increment(ref _totalGetCount);
                Interlocked.Add(ref _totalGetTime, endTime - startTime);
            }
        }
        
        /// <summary>
        /// 获取数据库连接（带超时）
        /// </summary>
        public IDatabase GetConnection(TimeSpan timeout)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(RedisConnectionPool));
            
            var startTime = Environment.TickCount64;
            
            try
            {
                if (!_semaphore.Wait(timeout))
                    throw new TimeoutException($"获取Redis连接超时({timeout.TotalSeconds}秒)");
                
                if (_disposed)
                    throw new ObjectDisposedException(nameof(RedisConnectionPool));
                
                if (_connections.TryDequeue(out var connection))
                {
                    Interlocked.Decrement(ref _availableCount);
                    return connection;
                }
                
                return CreateNewConnection();
            }
            finally
            {
                var endTime = Environment.TickCount64;
                Interlocked.Increment(ref _totalGetCount);
                Interlocked.Add(ref _totalGetTime, endTime - startTime);
            }
        }
        
        /// <summary>
        /// 异步获取数据库连接
        /// </summary>
        public async ETTask<IDatabase> GetConnectionAsync()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(RedisConnectionPool));
            
            var startTime = Environment.TickCount64;
            
            try
            {
                await _semaphore.WaitAsync();
                
                if (_disposed)
                    throw new ObjectDisposedException(nameof(RedisConnectionPool));
                
                if (_connections.TryDequeue(out var connection))
                {
                    Interlocked.Decrement(ref _availableCount);
                    return connection;
                }
                
                return CreateNewConnection();
            }
            finally
            {
                var endTime = Environment.TickCount64;
                Interlocked.Increment(ref _totalGetCount);
                Interlocked.Add(ref _totalGetTime, endTime - startTime);
            }
        }
        
        /// <summary>
        /// 释放连接回池
        /// </summary>
        public void ReleaseConnection(IDatabase connection)
        {
            if (connection == null || _disposed)
                return;
            
            try
            {
                if (_multiplexer.IsConnected)
                {
                    _connections.Enqueue(connection);
                    Interlocked.Increment(ref _availableCount);
                }
                else
                {
                    Interlocked.Decrement(ref _createdCount);
                }
            }
            catch (Exception ex)
            {
                Log.Debug($"释放Redis连接异常: {ex.Message}");
                Interlocked.Decrement(ref _createdCount);
            }
            finally
            {
                _semaphore.Release();
            }
        }
        
        /// <summary>
        /// 创建新连接
        /// </summary>
        private IDatabase CreateNewConnection()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(RedisConnectionPool));
            
            try
            {
                if (_multiplexer == null)
                    throw new InvalidOperationException("Redis连接复用器未初始化");
                
                var db = _multiplexer.GetDatabase();
                Interlocked.Increment(ref _createdCount);
                Log.Debug($"创建新Redis连接，当前总数: {_createdCount}");
                return db;
            }
            catch (Exception ex)
            {
                Log.Error($"创建Redis连接失败: {ex.Message}");
                throw new Exception($"创建Redis连接失败: {ex.Message}", ex);
            }
        }
        
        /// <summary>
        /// 运行健康检查
        /// </summary>
        public async ETTask RunHealthCheckAsync()
        {
            if (_disposed) 
                return;
            
            try
            {
                Log.Debug("开始Redis连接池健康检查...");
                
                // 检查连接状态
                if (!_multiplexer.IsConnected)
                {
                    Log.Warning("Redis连接已断开，尝试重连...");
                    await TryReconnectAsync();
                    return;
                }
                
                // 检查并修复死连接
                var connectionsToCheck = new IDatabase[AvailableConnections];
                int checkCount = 0;
                
                while (_connections.TryDequeue(out var conn) && checkCount < connectionsToCheck.Length)
                {
                    connectionsToCheck[checkCount++] = conn;
                    Interlocked.Decrement(ref _availableCount);
                }
                
                for (int i = 0; i < checkCount; i++)
                {
                    var conn = connectionsToCheck[i];
                    
                    try
                    {
                        // 快速Ping检查
                        var pingResult = await conn.PingAsync();
                        if (pingResult.TotalMilliseconds < 1000) // 1秒内响应
                        {
                            _connections.Enqueue(conn);
                            Interlocked.Increment(ref _availableCount);
                        }
                        else
                        {
                            Interlocked.Decrement(ref _createdCount);
                            Log.Debug("健康检查移除慢连接");
                        }
                    }
                    catch
                    {
                        Interlocked.Decrement(ref _createdCount);
                        Log.Debug("健康检查移除死连接");
                    }
                }
                
                // 保持最小连接数
                while (ActiveConnections < _minSize && _connections.Count < _minSize)
                {
                    try
                    {
                        var newConn = CreateNewConnection();
                        _connections.Enqueue(newConn);
                        Interlocked.Increment(ref _availableCount);
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"健康检查创建连接失败: {ex.Message}");
                        break;
                    }
                }
                
                Log.Debug($"健康检查完成: 活跃={ActiveConnections}, 可用={AvailableConnections}, 已连接={_multiplexer.IsConnected}");
            }
            catch (Exception ex)
            {
                Log.Error($"健康检查异常: {ex}");
            }
        }
        
        /// <summary>
        /// 尝试重连
        /// </summary>
        private async ETTask TryReconnectAsync()
        {
            try
            {
                // 获取当前连接的本地副本
                var currentMultiplexer = _multiplexer;
                
                if (currentMultiplexer == null || currentMultiplexer.IsConnected)
                    return;
        
                Log.Info("尝试重新连接Redis...");
        
                // 创建新连接
                var newMultiplexer = await ConnectionMultiplexer.ConnectAsync(_config);
                SetupEventHandlers();
                
                // 安全替换连接
                var oldMultiplexer = Interlocked.Exchange(ref _multiplexer, newMultiplexer);
                
                // 关闭旧连接
                if (oldMultiplexer != null)
                {
                    try
                    {
                        oldMultiplexer.Close();
                        oldMultiplexer.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Log.Debug($"关闭旧Redis连接时异常: {ex.Message}");
                    }
                }
        
                // 清空连接池
                while (_connections.TryDequeue(out _))
                {
                    Interlocked.Decrement(ref _availableCount);
                    Interlocked.Decrement(ref _createdCount);
                }
        
                Interlocked.Increment(ref _totalReconnects);
                Log.Info("Redis重连成功");
            }
            catch (Exception ex)
            {
                Log.Error($"Redis重连失败: {ex.Message}");
            }
        }
        
        /// <summary>
        /// 强制重连所有连接
        /// </summary>
        public async ETTask ForceReconnectAllAsync()
        {
            Log.Info("强制重连所有Redis连接...");
            
            try
            {
                // 清空连接池
                while (_connections.TryDequeue(out _))
                {
                    Interlocked.Decrement(ref _availableCount);
                    Interlocked.Decrement(ref _createdCount);
                }
                
                // 获取当前连接
                var currentMultiplexer = _multiplexer;
                
                // 创建新连接
                var newMultiplexer = await ConnectionMultiplexer.ConnectAsync(_config);
                SetupEventHandlers();
                
                // 原子替换
                var oldMultiplexer = Interlocked.Exchange(ref _multiplexer, newMultiplexer);
                
                // 关闭旧连接
                if (oldMultiplexer != null)
                {
                    try
                    {
                        oldMultiplexer.Close();
                        oldMultiplexer.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Log.Debug($"强制重连时关闭旧连接异常: {ex.Message}");
                    }
                }
                
                Interlocked.Increment(ref _totalReconnects);
                
                // 重新创建最小连接数
                for (int i = 0; i < _minSize; i++)
                {
                    try
                    {
                        var conn = CreateNewConnection();
                        _connections.Enqueue(conn);
                        Interlocked.Increment(ref _availableCount);
                    }
                    catch (Exception ex)
                    {
                        Log.Error($"强制重连创建连接失败: {ex.Message}");
                    }
                }
                
                Log.Info($"强制重连完成，总重连次数: {TotalReconnects}");
            }
            catch (Exception ex)
            {
                Log.Error($"强制重连失败: {ex.Message}");
                throw;
            }
        }
        
        /// <summary>
        /// 预热连接池
        /// </summary>
        public async ETTask WarmupAsync(int connectionCount = -1)
        {
            if (_disposed) return;
            
            int count = connectionCount > 0 ? 
                Math.Min(connectionCount, _maxSize) : _minSize;
            
            var connections = new List<IDatabase>();
            try
            {
                for (int i = 0; i < count; i++)
                {
                    var conn = GetConnection();
                    await conn.PingAsync();
                    connections.Add(conn);
                }
                Log.Info($"Redis连接池预热完成，预热连接数: {count}");
            }
            finally
            {
                foreach (var conn in connections)
                {
                    ReleaseConnection(conn);
                }
            }
        }
        
        /// <summary>
        /// 获取服务器信息
        /// </summary>
        public IServer GetServer()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(RedisConnectionPool));
            
            try
            {
                if (_multiplexer == null) return null;
                
                var endpoints = _multiplexer.GetEndPoints();
                if (endpoints.Length == 0)
                    return null;
                
                return _multiplexer.GetServer(endpoints.First());
            }
            catch (Exception ex)
            {
                Log.Error($"获取Redis服务器失败: {ex.Message}");
                return null;
            }
        }
        
        /// <summary>
        /// 获取发布订阅对象
        /// </summary>
        public ISubscriber GetSubscriber()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(RedisConnectionPool));
            
            if (_multiplexer == null)
                throw new InvalidOperationException("Redis连接复用器未初始化");
            
            return _multiplexer.GetSubscriber();
        }
        
        /// <summary>
        /// 获取性能统计
        /// </summary>
        public (int Active, int Available, double AvgGetTimeMs) GetPoolStatistics()
        {
            var avgTime = _totalGetCount > 0 ? 
                _totalGetTime * 1.0 / _totalGetCount : 0;
            
            return (ActiveConnections, AvailableConnections, avgTime);
        }
        
        /// <summary>
        /// 获取状态信息
        /// </summary>
        public string GetStatus()
        {
            if (_disposed)
                return "连接池已释放";
            
            var stats = GetPoolStatistics();
            var serverStatus = "未知";
            
            try
            {
                var server = GetServer();
                serverStatus = server?.IsConnected == true ? "已连接" : "未连接";
            }
            catch
            {
                // 忽略
            }
            
            return $"连接池状态: 活跃={stats.Active}, 可用={stats.Available}, " +
                   $"队列={_connections.Count}, 信号量={_semaphore.CurrentCount}, " +
                   $"服务器={serverStatus}\n" +
                   $"统计: 总重连={TotalReconnects}, 平均获取时间={stats.AvgGetTimeMs:F1}ms";
        }
        
        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            if (_disposed) 
                return;
            
            _disposed = true;
            
            try
            {
                // 清空连接池
                while (_connections.TryDequeue(out _))
                {
                    Interlocked.Decrement(ref _availableCount);
                    Interlocked.Decrement(ref _createdCount);
                }
                
                // 关闭连接复用器
                _multiplexer?.Close();
                _multiplexer?.Dispose();
                
                // 释放信号量
                _semaphore?.Dispose();
                
                Log.Info("Redis连接池已释放");
            }
            catch (Exception ex)
            {
                Log.Error($"释放Redis连接池异常: {ex.Message}");
            }
        }
    }
}