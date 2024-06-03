/**
 * @file redis.h
 * @author Keisum (Keisumhuis@gmail.com)
 * @brief 
 * @version 0.1
 * @date 2024-05-30
 * 
 * @copyright Copyright (c) 2024
 * 
 */
#ifndef ____REDIS_H____
#define ____REDIS_H____

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "hiredis.h"

enum class RedisDataType : int8_t {
    none,
    string,
    list,
    set,
    zset,
    hash
};

struct RedisReplyDistory {
    void operator()(redisReply* reply) {
        if (reply) freeReplyObject(reply);
    }
};

using RedisReplyPtr = std::unique_ptr<redisReply, RedisReplyDistory>;

class RedisClient {
public:
    using Ptr = std::shared_ptr<RedisClient>;
    static RedisClient::Ptr Create(const std::string& ip = "127.0.0.1"
        , const uint16_t port = 6379, const std::string& password = "");
    
    RedisClient();
    RedisClient(const std::string& ip, const uint16_t port, const std::string& password = "");

    bool Reconnect();
    bool Connect();
    bool Connect(const std::string& ip, const uint16_t port, const std::string& password = "");
    bool ConnectWithTimeout(uint64_t ms);
    bool ConnectWithTimeout(const std::string& ip, const uint16_t port, uint64_t ms, const std::string& password = "");
    void SetPassword(const std::string& password);
    std::string GetPassword() const;

    RedisReplyPtr Command(const char* fmt, ...);
    RedisReplyPtr Command(const char* fmt, va_list ap);

    /** key             */
    /** DEL             */ int32_t del(const std::string& key);
    /** DUMP            */ std::optional<std::string> dump(const std::string& key);
    /** EXISTS          */ bool exists(const std::string& key);
    /** Expire          */ bool expire(const std::string& key, const int64_t seconds);
    /** EXPIREAT        */ bool expireat(const std::string& key, const int64_t unix_timestamp);
    /** PEXPIRE         */ bool pexpire(const std::string& key, const int64_t milliseconds);
    /** PEXPIREAT       */ bool pexpireat(const std::string& key, const int64_t milliseconds_timestamp);
    /** KEYS            */ std::vector<std::string> keys(const std::string& pattern);
    /** MOVE            */ bool move(const std::string& key, const int32_t destination_database);
    /** PERSIST         */ bool persist(const std::string& key);
    /** PTTL            */ int32_t pttl(const std::string& key);
    /** TTL             */ int32_t ttl(const std::string& key);
    /** RANDOMKEY       */ std::optional<std::string> randonkey();
    /** RENAME          */ bool rename(const std::string& old_key, const std::string& new_key);
    /** RENAMENX        */ bool renamenx(const std::string& old_key, const std::string& new_key);
    /** TYPE            */ RedisDataType type(const std::string& key);

    /** string          */
    /** SET             */ bool set(const std::string& key, const std::string& value);
    /** GET             */ std::optional<std::string> get(const std::string& key);
    /** GETRANGE        */ std::optional<std::string> getrange(const std::string& key, int32_t start, int32_t end);
    /** GETSET          */ std::optional<std::string> getset(const std::string& key, const std::string& value);
    /** GETBIT          */ std::optional<int32_t> getbit(const std::string& key, int32_t offset);
    /** MGET            */ std::vector<std::optional<std::string>> mget(const std::vector<std::string>& keys);
    /** SETBIT          */ bool setbit(const std::string& key, int32_t offset, int32_t bit);
    /** SETEX           */ bool setex(const std::string& key, int32_t seconds, const std::string& value);
    /** SETNX           */ bool setnx(const std::string& key, const std::string& value);
    /** SETRANGE        */ bool setrange(const std::string& key, int32_t offset, const std::string& value);
    /** STRLEN          */ std::optional<int32_t> strlen(const std::string& key);
    /** MSET            */ bool mset(const std::vector<std::pair<std::string, std::string>>& values);
    /** INCR            */ int64_t incr(const std::string& key);
    /** INCRBY          */ int64_t incrby(const std::string& key, int64_t increment);
    /** INCRBYFLOAT     */ double incrbyfloat(const std::string& key, double increment);
    /** DECR            */ int64_t decr(const std::string& key);
    /** DECRBY          */ int64_t decrby(const std::string& key, int64_t decrement);
    /** APPEND          */ int64_t append(const std::string& key, const std::string& value);

    /** hash            */
    /** HDEL            */ bool hdel(const std::string& key, const std::vector<std::string>& fields);
    /** HEXISTS         */ bool hexists(const std::string& key, const std::string& field);
    /** HGET            */ std::optional<std::string> hget(const std::string& key, const std::string& field);
    /** HGETALL         */ std::unordered_map<std::string, std::string> hgetall(const std::string& key);
    /** HINCRBY         */ int64_t hincrby(const std::string& key, const std::string& field, int64_t increment);
    /** HINCRBYFLOAT    */ double hicrbyfloat(const std::string& key, const std::string& field, double increment);
    /** HKEYS           */ std::vector<std::string> hkeys(const std::string& key);
    /** HLEN            */ int64_t hlen(const std::string& key);
    /** HMGET           */ std::vector<std::optional<std::string>> hmget(const std::string& key, const std::vector<std::string>& fields);
    /** HMSET           */ bool hmset(const std::string& key, const std::unordered_map<std::string, std::string>& values);
    /** HSET            */ bool hset(const std::string& key, const std::string& field, const std::string& value);
    /** HSETNX          */ bool hsetnx(const std::string& key, const std::string& field, const std::string& value);
    /** HVALS           */ std::vector<std::string> hvals(const std::string& key);

    /** set             */
    /** SADD            */ bool sadd(const std::string& key, const std::vector<std::string>& members, int& addedCount);
    /** SCARD           */ int64_t scard(const std::string& key);
    /** SDIFF           */ std::vector<std::string> sdiff(const std::vector<std::string>& keys);
    /** SDIFFSTORE      */ bool sdiffstore(const std::string& destination, const std::vector<std::string>& keys);
    /** SINTER          */ std::vector<std::string> sinter(const std::vector<std::string>& keys);
    /** SINTERSTORE     */ bool sinterstore(const std::string& destination, const std::vector<std::string>& keys);
    /** SISMEMBER       */ bool sismember(const std::string& key, const std::string& member);
    /** SMEMBERS        */ std::vector<std::string> smembers(const std::string& key);
    /** SMOVE           */ bool smove(const std::string& source, const std::string& destination, const std::string& member);
    /** SPOP            */ std::optional<std::string> spop(const std::string& key);
    /** SRANDMEMBER     */ std::vector<std::string> srandmember(const std::string& key, size_t count);
    /** SREM            */ bool srem(const std::string& key, const std::vector<std::string>& members, int& removedCount);
    /** SUNION          */ std::vector<std::string> sunion(const std::vector<std::string>& keys);
    /** SUNIONSTORE     */ bool sunionstore(const std::string& destination, const std::vector<std::string>& keys);

    /** sorted set      */
    /** ZADD            */ bool zadd(const std::string& key, const std::vector<std::pair<std::string, double>>& membersWithScores, int& addedCount);
    /** ACARD           */ int64_t zcard(const std::string& key);
    /** ZCOUNT          */ int64_t zcount(const std::string& key, double minScore, double maxScore);
    /** ZINCRBY         */ double zincrby(const std::string& key, double increment, const std::string& member);
    /** ZINTERSTORE     */ bool zinterstore(const std::string& destination, const std::vector<std::string>& keys, const std::vector<std::string>& weights, bool aggregateSum, int64_t& count);
    /** ZLEXCOUNT       */ int64_t zlexcount(const std::string& key, const std::string& minMember, const std::string& maxMember);
    /** ZRANGE          */ std::vector<std::string> zrange(const std::string& key, int start, int stop, bool withScores);
    /** ZRANGEBYLEX     */ std::vector<std::string> zrangebylex(const std::string& key, const std::string& minLex, const std::string& maxLex, bool withScores, long long offset, long long count);
    /** ZRANGEBYSCORE   */ std::vector<std::string> zrangebyscore(const std::string& key, double minScore, double maxScore, bool withScores, bool reverse, long long limitOffset, long long limitCount);
    /** ZRANK           */ int64_t zrank(const std::string& key, const std::string& member);
    /** ZREMRANGEBYLEX  */ int64_t zremrangebylex(const std::string& key, const std::string& minLex, const std::string& maxLex);
    /** ZREMRANGEBYRANK */ int64_t zremrangebyrank(const std::string& key, int start, int stop);
    /** ZREMRANGEBYSCORE*/ int64_t zremrangebyscore(const std::string& key, double minScore, double maxScore);
    /** ZREVRANGE       */ std::vector<std::string> zrevrange(const std::string& key, int start, int stop, bool withscores);
    /** ZREVRANGEBYSCORE*/ std::vector<std::string> zrevrangebyscore(const std::string& key, double maxScore, double minScore, bool withScores, int offset, int count);
    /** ZREVRANK        */ int64_t zrevrank(const std::string& key, const std::string& member);
    /** ZSCORE          */ std::optional<double> zscore(const std::string& key, const std::string& member);
    /** ZUNIONSTORE     */ bool zunionstore(const std::string& destination, const std::vector<std::string>& keys, const std::vector<double>& weights /* = {} */, const std::string& aggregate /* = "SUM" */);

    /** list            */
    /** BLPOP           */ std::pair<std::string, std::string> blpop(const std::vector<std::string>& keys, int timeout);
    /** BRPOP           */ std::pair<std::string, std::string> brpop(const std::vector<std::string>& keys, int timeout);
    /** BRPOPLPUSH      */ std::string brpoplpush(const std::string& source, const std::string& destination, int timeout);
    /** LINDEX          */ std::optional<std::string> lindex(const std::string& key, long long index);
    /** LINSERT         */ bool linsert(const std::string& key, const std::string& pivot, const std::string& value, bool before);
    /** LLEN            */ size_t llen(const std::string& key);
    /** LPOP            */ std::optional<std::string> lpop(const std::string& key);
    /** LPUSH           */ long long lpush(const std::string& key, const std::vector<std::string>& values);
    /** LPUSHX          */ long long lpushx(const std::string& key, const std::string& value);
    /** LRANGE          */ std::vector<std::string> lrange(const std::string& key, long long start, long long stop);
    /** LREM            */ int64_t lrem(const std::string& key, int count, const std::string& value);
    /** LSET            */ bool lset(const std::string& key, long long index, const std::string& value);
    /** LTRIM           */ bool ltrim(const std::string& key, long long start, long long stop);
    /** RPOP            */ std::optional<std::string> rpop(const std::string& key);
    /** RPOPLPUSH       */ std::string rpoplpush(const std::string& source, const std::string& destination);
    /** RPUSH           */ long long rpush(const std::string& key, const std::vector<std::string>& values);
    /** RPUSHX          */ long long rpushx(const std::string& key, const std::string& value);
private:
    std::string m_host;
    uint16_t m_port;
    std::string m_password;
    std::shared_ptr<redisContext> m_context;
};

class RedisConnectPoolGuard;
class RedisConnectPool final {
    friend class RedisConnectPoolGuard;
public:
    using Ptr = std::shared_ptr<RedisConnectPool>;
    RedisConnectPool() {}
    ~RedisConnectPool() {}
    static RedisConnectPool::Ptr Instance() {
        static auto p = std::make_shared<RedisConnectPool>();
        return p;
    }
    void Connect(const std::string& ip, const uint16_t port
        , size_t count = std::thread::hardware_concurrency(), int64_t ms = 50, const std::string& password = "") {
        count = count ? count : 1;
        m_connections.resize(count ? count : 1);
        for (auto i = 0; i < count; ++i) {
            auto conn = std::make_shared<RedisClient>();
            conn->ConnectWithTimeout(ip, port, ms, password);
            m_connections.push_back(conn);
            m_freeconnections.push_back(conn);
        }
    }
    size_t ConnectPoolSize() const { return m_connections.size(); }
    size_t FreeConnectionSize() const { return m_freeconnections.size(); }
protected:
    RedisClient::Ptr Get() {
        std::lock_guard guard(m_mutex);
        if (!m_freeconnections.size()) {
            throw std::runtime_error("without redis connection");
        }
        auto conn = m_freeconnections.begin();
        m_freeconnections.erase(m_freeconnections.begin());
        return *conn;
    }
    void Return(RedisClient::Ptr conn) {
        std::lock_guard guard(m_mutex);
        m_freeconnections.push_back(conn);
    }
private:
    std::mutex m_mutex;
    std::vector<RedisClient::Ptr> m_connections;
    std::vector<RedisClient::Ptr> m_freeconnections;
};

class RedisConnectPoolGuard final {
public:
    ~RedisConnectPoolGuard() {
        RedisConnectPool::Instance()->Return(m_conn);
    }
    RedisClient::Ptr Get() {
        m_conn = RedisConnectPool::Instance()->Get();
        return m_conn;
    }
private:
    RedisClient::Ptr m_conn;
};

#endif // ! ____REDIS_H____