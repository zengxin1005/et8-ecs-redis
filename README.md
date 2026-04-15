# et8-ecs-redis
基于et8 ecs 的Redis驱动


public static RedisComponent GetZoneRedis(this DBManagerComponent self, int zone)
{
    RedisComponent redisComponent = self.GetChild<RedisComponent>(zone);
    if (redisComponent != null)
    {
        return redisComponent;
    }
    return null;
}
public static RedisComponent CreateZoneRedis(this DBManagerComponent self, int zone, string connectionString, int databaseNumber)
{
    /*
        connectionString 说明：
    1. 单机模式（最常用）

    "127.0.0.1:6379,password=123456,defaultDatabase=0,connectTimeout=5000,syncTimeout=5000"
    2. 无密码（本地开发）

    "127.0.0.1:6379,defaultDatabase=0"
    3. 带密码和连接池

    "127.0.0.1:6379,password=123456,defaultDatabase=0,connectTimeout=5000,syncTimeout=5000,abortConnect=false"
    4. 集群模式

    "server1:6379,server2:6379,server3:6379,password=123456,defaultDatabase=0,abortConnect=false"
    5. 云服务（如阿里云）
    "r-xxxxxxxxxx.redis.rds.aliyuncs.com:6379,password=xxxxxx,defaultDatabase=0,ssl=true"
        
    databaseNumber 说明：
    Redis 默认有 16 个数据库（0-15），建议：

    0：缓存数据（可丢失）

    1：业务数据（排行榜、在线状态）

    2：分布式锁

    3：消息队列

    4-15：备用
    */
    RedisComponent redisComponent = self.GetChild<RedisComponent>(zone);
    if (redisComponent != null)
    {
        throw new Exception($"zone: {zone} already created redis");
    }
    return self.AddChildWithId<RedisComponent, string, int>(zone, connectionString, databaseNumber);
}
