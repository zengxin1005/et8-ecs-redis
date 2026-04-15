/*
 * 文件名: RedisComponent.cs
 * 作者: zengxin
 * 创建日期: 2026-2-25
 */

using StackExchange.Redis;

namespace ET.Server
{
    [ChildOf(typeof(DBManagerComponent))]
    public class RedisComponent: Entity, IAwake<string, int>, IDestroy
    {
        public string ConnectionString { get; set; }
        public int DatabaseNumber { get; set; }
        
        // StackExchange.Redis 的连接 multiplexer（官方推荐单例）
        public ConnectionMultiplexer Multiplexer { get; set; }
        
        // 获取数据库的快捷方式
        public IDatabase Database => Multiplexer?.GetDatabase(DatabaseNumber);
        
        // 服务器信息（用于集群操作）
        public IServer Server => Multiplexer?.GetServer(Multiplexer.GetEndPoints()[0]);
    }
}