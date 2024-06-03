#include "redis.h"
#include <iostream>

RedisClient::Ptr RedisClient::Create(const std::string& ip
        , const uint16_t port, const std::string& password) {
    auto redisClient = std::make_shared<RedisClient>(ip, port, password);
    return redisClient;
}
RedisClient::RedisClient() 
    : m_host (""), m_port (0), m_password (""), m_context (nullptr) {
}
RedisClient::RedisClient(const std::string& ip, const uint16_t port, const std::string& password) 
    : m_host (ip), m_port (port), m_password (password), m_context (nullptr){
}
bool RedisClient::Reconnect() {
    return redisReconnect(m_context.get());
}
bool RedisClient::Connect() {
    return Connect(m_host, m_port, m_password);
}
bool RedisClient::Connect(const std::string& ip, const uint16_t port, const std::string& password) {
    return ConnectWithTimeout(ip, port, 50, password);
}
bool RedisClient::ConnectWithTimeout(uint64_t ms) {
    return ConnectWithTimeout(m_host, m_port, ms, m_password);
}
bool RedisClient::ConnectWithTimeout(const std::string& ip, const uint16_t port, uint64_t ms, const std::string& password) {
    m_host = ip, m_port = port, m_password = password;

    timeval tv {(long long)ms / 1000, (int)ms % 1000 * 1000};
    auto client = redisConnectWithTimeout(m_host.c_str(), m_port, tv);
    if (!client) {
        return false;
    }
    m_context.reset(client, redisFree);
    if (!m_password.empty()) {
        auto rt = Command("auth %s", m_password.c_str());
        if (!rt) {
            throw std::runtime_error("auth error:( " + m_host + " : " + std::to_string(m_port));
        }
        if (rt->type != REDIS_REPLY_STATUS) {
            throw std::runtime_error("auth reply type error:( " + m_host + " : " + std::to_string(m_port));
        }
        if (!rt->str) {
            throw std::runtime_error("auth reply str error:( " + m_host + " : " + std::to_string(m_port) + ", " + rt->str);
        }
        if (!strcmp("OK", rt->str)) {
            return true;
        } else {
            throw std::runtime_error("auth error:( " + m_host + " : " + std::to_string(m_port));
        }
    }
    return true;
}
void RedisClient::SetPassword(const std::string& password) {
    m_password = password;
}
std::string RedisClient::GetPassword() const {
    return m_password;
}
RedisReplyPtr RedisClient::Command(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    auto r = Command(fmt, ap);
    va_end(ap);
    return r;
}
RedisReplyPtr RedisClient::Command(const char* fmt, va_list ap) {
    auto reply = (redisReply*)redisvCommand(m_context.get(), fmt, ap);
    return std::unique_ptr<redisReply, RedisReplyDistory>(reply);
}
int32_t RedisClient::del(const std::string& key) {
    auto reply = Command("DEL %s", key.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis error, command : del " + key
            + ", error message : " + reply->str);
    }
    return reply->integer;
}
std::optional<std::string> RedisClient::dump(const std::string& key) {
    RedisReplyPtr reply = Command("DUMP %s", key.c_str());
    if (!reply || reply->type == REDIS_REPLY_NIL) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command : DUMP " + key
                + ", error message : " + reply->str);
        }
        return {}; 
    } else if (reply->type != REDIS_REPLY_STRING) {
        throw std::runtime_error("Unexpected reply type when executing DUMP for key: " + key);
    }
    return std::string(reply->str, reply->len);
}
bool RedisClient::exists(const std::string& key) {
     auto reply = Command("EXISTS %s", key.c_str());
     return reply && !reply->type == REDIS_REPLY_ERROR && reply->integer;
}
bool RedisClient::expire(const std::string& key, const int64_t seconds) {
    auto reply = Command("EXPIRE %s %lld", key.c_str(), seconds);
    return reply && !reply->type == REDIS_REPLY_ERROR && reply->integer;
}
bool RedisClient::expireat(const std::string& key, const int64_t unix_timestamp) {
    auto reply = Command("EXPIREAT %s %lld", key.c_str(), unix_timestamp);
    return reply && !reply->type == REDIS_REPLY_ERROR && reply->integer;
}
bool RedisClient::pexpire(const std::string& key, const int64_t milliseconds) {
    auto reply = Command("PEXPIRE %s %lld", key.c_str(), milliseconds);
    return reply && !reply->type == REDIS_REPLY_ERROR && reply->integer;
}
bool RedisClient::pexpireat(const std::string& key, const int64_t milliseconds_timestamp) {
    auto reply = Command("PEXPIREAT %s %lld", key.c_str(), milliseconds_timestamp);
    return reply && !reply->type == REDIS_REPLY_ERROR && reply->integer;
}
std::vector<std::string> RedisClient::keys(const std::string& pattern) {
    auto reply = Command("KEYS %s", pattern.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis error, command : keys " + pattern
            + ", error message : " + reply->str);
    }
    std::vector<std::string> keys;
    for (size_t i = 0; i < reply->elements; ++i) {
        keys.push_back(reply->element[i]->str);
    }
    return keys;
}
bool RedisClient::move(const std::string& key, const int32_t destination_database) {
    auto reply = Command("MOVE %s %d", key.c_str(), destination_database);
    return reply && !reply->type == REDIS_REPLY_ERROR && reply->integer;
}
bool RedisClient::persist(const std::string& key) {
    auto reply = Command("PERSIST %s", key.c_str());
    return reply && !reply->type == REDIS_REPLY_ERROR && reply->integer;
}
int32_t RedisClient::pttl(const std::string& key) {
    auto reply = Command("PTTL %s", key.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis error, command : pttl " + key
            + ", error message : " + reply->str);
    }
    return reply->integer;
}
int32_t RedisClient::ttl(const std::string& key) {
    auto reply = Command("TTL %s", key.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis error, command : ttl " + key
            + ", error message : " + reply->str);
    }
    return reply->integer;
}
std::optional<std::string> RedisClient::randonkey() {
    auto reply = Command("RANDOMKEY");
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        return {};
    }
    return std::optional<std::string>(reply->str);
}
bool RedisClient::rename(const std::string& old_key, const std::string& new_key) {
    auto reply = Command("RENAME %s %s", old_key.c_str(), new_key.c_str());
    return reply && !reply->type == REDIS_REPLY_ERROR;
}
bool RedisClient::renamenx(const std::string& old_key, const std::string& new_key) {
    auto reply = Command("RENAMENX %s %s", old_key.c_str(), new_key.c_str());
    return reply && !reply->type == REDIS_REPLY_ERROR && reply->integer;
}
RedisDataType RedisClient::type(const std::string& key) {
    auto reply = Command("TYPE %s", key.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis error, command : type " + key
            + ", error message : " + reply->str);
    }
    if (!strcmp(reply->str, "none")) return RedisDataType::none;
    if (!strcmp(reply->str, "string")) return RedisDataType::string;
    if (!strcmp(reply->str, "list")) return RedisDataType::list;
    if (!strcmp(reply->str, "set")) return RedisDataType::set;
    if (!strcmp(reply->str, "zset")) return RedisDataType::zset;
    if (!strcmp(reply->str, "hash")) return RedisDataType::hash;
    throw std::runtime_error("redis error, command : type " + key
            + ", error message : invalid type (" + reply->str + ")");
}
bool RedisClient::set(const std::string& key, const std::string& value) {
    auto reply = Command("SET %s %s", key.c_str(), value.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis error, command : SET " + key + " " + value
            + ", error message : " + reply->str);
    }
    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
        return true;
    } else if (reply->type == REDIS_REPLY_NIL) {
        return true;
    } else {
        throw std::runtime_error("Unexpected reply type when executing SET for key: " + key);
    }
}
std::optional<std::string> RedisClient::get(const std::string& key) {
    auto reply = Command("GET %s", key.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("redis error, command : GET " + key
                + ", error message : " + reply->str);
        }
        return {};
    }
    if (reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len);
    } else if (reply->type == REDIS_REPLY_NIL) {
        return {};
    } else {
        throw std::runtime_error("Unexpected reply type when executing GET for key: " + key);
    }
}
std::optional<std::string> RedisClient::getrange(const std::string& key, int32_t start, int32_t end) {
    auto reply = Command("GETRANGE %s %d %d", key.c_str(), start, end);
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command : GETRANGE " + key + " " + std::to_string(start) 
                + " " + std::to_string(end) + ", error message : " + reply->str);
        }
        return {};
    }
    if (reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len);
    } else {
        throw std::runtime_error("Unexpected reply type when executing GETRANGE for key: " + key);
    }
}
std::optional<std::string> RedisClient::getset(const std::string& key, const std::string& value) {
    auto reply = Command("GETSET %s %s", key.c_str(), value.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: GETSET " + key + " " + value 
                + ", error message: " + reply->str);
        }
        return {}; 
    }
    if (reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len);
    } else {
        throw std::runtime_error("Unexpected reply type when executing GETSET for key: " + key);
    }
}
std::optional<int32_t> RedisClient::getbit(const std::string& key, int32_t offset) {
    auto reply = Command("GETBIT %s %d", key.c_str(), offset);
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: GETBIT " + key + " " + std::to_string(offset) 
                + ", error message: " + reply->str);
        }
        return {};
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply type when executing GETBIT for key: " + key);
    }
}
std::vector<std::optional<std::string>> RedisClient::mget(const std::vector<std::string>& keys) {
    std::stringstream cmd;
    cmd << "MGET";
    for (const auto& key : keys) {
        cmd << " " << key;
    }
    auto reply = Command(cmd.str().c_str());
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: MGET " + cmd.str() 
                + ", error message: " + reply->str);
        } else {
            throw std::runtime_error("Unexpected reply type when executing MGET");
        }
    }
    std::vector<std::optional<std::string>> values;
    for (size_t i = 0; i < reply->elements; ++i) {
        auto element = reply->element[i];
        if (element->type == REDIS_REPLY_STRING) {
            values.push_back(std::string(element->str, element->len));
        } else if (element->type == REDIS_REPLY_NIL) {
            values.push_back(std::nullopt);
        } else {
            throw std::runtime_error("Unexpected element type within MGET reply");
        }
    }
    return values;
}
bool RedisClient::setbit(const std::string& key, int32_t offset, int32_t bit) {
    auto reply = Command("SETBIT %s %d %d", key.c_str(), offset, bit);
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: SETBIT " + key + " " + std::to_string(offset) 
                + " " + std::to_string(bit) + ", error message: " + reply->str);
        }
        return false;
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return true;
    } else {
        throw std::runtime_error("Unexpected reply type when executing SETBIT for key: " + key);
    }
}
bool RedisClient::setex(const std::string& key, int32_t seconds, const std::string& value) {
    auto reply = Command("SETEX %s %d %s", key.c_str(), seconds, value.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: SETEX " + key + " " + std::to_string(seconds) 
                + " " + value + ", error message: " + reply->str);
        }
        return false;
    }
    if (reply->type == REDIS_REPLY_STATUS && strcmp(reply->str, "OK") == 0) {
        return true;
    } else {
        throw std::runtime_error("Unexpected reply type or content when executing SETEX for key: " + key);
    }
}
bool RedisClient::setnx(const std::string& key, const std::string& value) {
    auto reply = Command("SETNX %s %s", key.c_str(), value.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: SETNX " + key + " " + value 
                + ", error message: " + reply->str);
        }
        return false;
    }
    if (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1) {
        return true; 
    } else if (reply->integer == 0) {
        return false;
    } else {
        throw std::runtime_error("Unexpected reply type or content when executing SETNX for key: " + key);
    }
}
bool RedisClient::setrange(const std::string& key, int32_t offset, const std::string& value) {
    auto reply = Command("SETRANGE %s %d %s", key.c_str(), offset, value.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: SETRANGE " + key + " " + std::to_string(offset) 
                + " " + value + ", error message: " + reply->str);
        }
        return false;
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return true;
    } else {
        throw std::runtime_error("Unexpected reply type when executing SETRANGE for key: " + key);
    }
}
std::optional<int32_t> RedisClient::strlen(const std::string& key) {
    auto reply = Command("STRLEN %s", key.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: STRLEN " + key 
                + ", error message: " + reply->str);
        }
        return {};
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply type when executing STRLEN for key: " + key);
    }
}
bool RedisClient::mset(const std::vector<std::pair<std::string, std::string>>& values) {
    std::stringstream cmd;
    cmd << "MSET";
    for (const auto& pair : values) {
        cmd << " " << pair.first << " " << pair.second;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (!reply || reply->type != REDIS_REPLY_STATUS || strcmp(reply->str, "OK")) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: MSET " + cmd.str() 
                + ", error message: " + reply->str);
        } else {
            throw std::runtime_error("Unexpected reply or failed to execute MSET");
        }
    }
    return true;
}
int64_t RedisClient::incr(const std::string& key) {
    auto reply = Command("INCR %s", key.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: INCR " + key 
                + ", error message: " + reply->str);
        }
        throw std::runtime_error("Failed to increment key without specific error message");
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply type when executing INCR for key: " + key);
    }
}
int64_t RedisClient::incrby(const std::string& key, int64_t increment) {
    std::stringstream cmd;
    cmd << "INCRBY " << key << " " << increment;
    auto reply = Command(cmd.str().c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: INCRBY " + key + " " + std::to_string(increment) 
                + ", error message: " + reply->str);
        }
        throw std::runtime_error("Failed to increment key by specified amount without specific error message");
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply type when executing INCRBY for key: " + key);
    }
}
double RedisClient::incrbyfloat(const std::string& key, double increment) {
    std::stringstream cmd;
    cmd << "INCRBYFLOAT " << key << " " << increment;
    auto reply = Command(cmd.str().c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: INCRBYFLOAT " + key + " " + std::to_string(increment)
                + ", error message: " + reply->str);
        }
        throw std::runtime_error("Failed to increment key by float value without specific error message");
    }
    return std::stod(reply->str);
}
int64_t RedisClient::decr(const std::string& key) {
    std::stringstream cmd;
    cmd << "DECR " << key;
    auto reply = Command(cmd.str().c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: DECR " + key 
                + ", error message: " + reply->str);
        }
        throw std::runtime_error("Failed to decrement key without specific error message");
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply type when executing DECR for key: " + key);
    }
}
int64_t RedisClient::decrby(const std::string& key, int64_t decrement) {
    std::stringstream cmd;
    cmd << "DECRBY " << key << " " << decrement;
    auto reply = Command(cmd.str().c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: DECRBY " + key + " " + std::to_string(decrement) 
                + ", error message: " + reply->str);
        }
        throw std::runtime_error("Failed to decrement key by specified amount without specific error message");
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply type when executing DECRBY for key: " + key);
    }
}
int64_t RedisClient::append(const std::string& key, const std::string& value) {
    std::stringstream cmd;
    cmd << "APPEND " << key << " " << value;
    auto reply = Command(cmd.str().c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply && reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("Redis error, command: APPEND " + key + " " + value 
                + ", error message: " + reply->str);
        }
        throw std::runtime_error("Failed to append value to key without specific error message");
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply type when executing APPEND for key: " + key);
    }
}
bool RedisClient::hdel(const std::string& key, const std::vector<std::string>& fields) {
    std::stringstream cmd;
    cmd << "HDEL " << key;
    for (const auto& field : fields) {
        cmd << " " << field;
    }
    
    auto reply = Command(cmd.str().c_str());
    return reply && reply->type == REDIS_REPLY_INTEGER && reply->integer > 0;
}
bool RedisClient::hexists(const std::string& key, const std::string& field) {
    auto reply = Command("HEXISTS %s %s", key.c_str(), field.c_str());
    return reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
}
std::optional<std::string> RedisClient::hget(const std::string& key, const std::string& field) {
    auto reply = Command("HGET %s %s", key.c_str(), field.c_str());
    if (reply && reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len);
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        return std::nullopt; 
    }
    throw std::runtime_error("Unexpected reply type when executing HGET for key: " + key);
}
std::unordered_map<std::string, std::string> RedisClient::hgetall(const std::string& key) {
    std::unordered_map<std::string, std::string> result;
    auto reply = Command("HGETALL %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
        throw std::runtime_error("Unexpected reply when executing HGETALL for key: " + key);
    }
    for (size_t i = 0; i < reply->elements; i += 2) {
        auto fieldReply = reply->element[i];
        auto valueReply = reply->element[i + 1];
        if (fieldReply->type == REDIS_REPLY_STRING && valueReply->type == REDIS_REPLY_STRING) {
            result[fieldReply->str] = std::string(valueReply->str, valueReply->len);
        } else {
            throw std::runtime_error("Invalid pair in HGETALL reply for key: " + key);
        }
    }
    return result;
}
int64_t RedisClient::hincrby(const std::string& key, const std::string& field, int64_t increment) {
    auto reply = Command("HINCRBY %s %s %lld", key.c_str(), field.c_str(), increment);
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        throw std::runtime_error("Unexpected reply when executing HINCRBY for key: " + key);
    }
    return reply->integer;
}
double RedisClient::hicrbyfloat(const std::string& key, const std::string& field, double increment) {
    auto reply = Command("HINCRBYFLOAT %s %s %f", key.c_str(), field.c_str(), increment);
    if (!reply || reply->type != REDIS_REPLY_STRING) {
        throw std::runtime_error("Unexpected reply when executing HINCRBYFLOAT for key: " + key);
    }
    return std::stod(reply->str);
}
std::vector<std::string> RedisClient::hkeys(const std::string& key) {
    std::vector<std::string> keys;
    auto reply = Command("HKEYS %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
        throw std::runtime_error("Unexpected reply when executing HKEYS for key: " + key);
    }
    for (size_t i = 0; i < reply->elements; ++i) {
        auto fieldReply = reply->element[i];
        if (fieldReply->type == REDIS_REPLY_STRING) {
            keys.push_back(std::string(fieldReply->str, fieldReply->len));
        } else {
            throw std::runtime_error("Invalid entry in HKEYS reply for key: " + key);
        }
    }
    return keys;
}
int64_t RedisClient::hlen(const std::string& key) {
    auto reply = Command("HLEN %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        throw std::runtime_error("Unexpected reply when executing HLEN for key: " + key);
    }
    return reply->integer;
}
std::vector<std::optional<std::string>> RedisClient::hmget(const std::string& key, const std::vector<std::string>& fields) {
    std::stringstream cmd;
    cmd << "HMGET " << key;
    for (const auto& field : fields) {
        cmd << " " << field;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
        throw std::runtime_error("Unexpected reply when executing HMGET for key: " + key);
    }
    std::vector<std::optional<std::string>> values;
    for (size_t i = 0; i < reply->elements; ++i) {
        auto fieldReply = reply->element[i];
        if (fieldReply->type == REDIS_REPLY_NIL) {
            values.push_back(std::nullopt);
        } else if (fieldReply->type == REDIS_REPLY_STRING) {
            values.push_back(std::string(fieldReply->str, fieldReply->len));
        } else {
            throw std::runtime_error("Invalid entry in HMGET reply for key: " + key);
        }
    }
    return values;
}
bool RedisClient::hmset(const std::string& key, const std::unordered_map<std::string, std::string>& values) {
    std::stringstream cmd;
    cmd << "HMSET " << key;
    for (const auto& pair : values) {
        cmd << " " << pair.first << " " << pair.second;
    }
    
    auto reply = Command(cmd.str().c_str());
    return reply && reply->type == REDIS_REPLY_STRING && strcmp(reply->str, "OK") == 0;
}
bool RedisClient::hset(const std::string& key, const std::string& field, const std::string& value) {
    auto reply = Command("HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
    return reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
}
bool RedisClient::hsetnx(const std::string& key, const std::string& field, const std::string& value) {
    auto reply = Command("HSETNX %s %s %s %s", key.c_str(), field.c_str(), value.c_str());
    return reply && reply->type == REDIS_REPLY_INTEGER && reply->integer == 1;
}
std::vector<std::string> RedisClient::hvals(const std::string& key) {
    std::vector<std::string> values;
    auto reply = Command("HVALS %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
        throw std::runtime_error("Unexpected reply when executing HVALS for key: " + key);
    }
    for (size_t i = 0; i < reply->elements; ++i) {
        auto valueReply = reply->element[i];
        if (valueReply->type == REDIS_REPLY_STRING) {
            values.push_back(std::string(valueReply->str, valueReply->len));
        } else {
            throw std::runtime_error("Invalid entry in HVALS reply for key: " + key);
        }
    }
    return values;
}
bool RedisClient::sadd(const std::string& key, const std::vector<std::string>& members, int& addedCount) {
    std::stringstream cmd;
    cmd << "SADD " << key;
    for (const auto& member : members) {
        cmd << " " << member;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        addedCount = reply->integer;
        return true;
    } else {
        throw std::runtime_error("Unexpected reply when executing SADD for key: " + key);
    }
}
int64_t RedisClient::scard(const std::string& key) {
    auto reply = Command("SCARD %s", key.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply when executing SCARD for key: " + key);
    }
}
std::vector<std::string> RedisClient::sdiff(const std::vector<std::string>& keys) {
    std::stringstream cmd;
    cmd << "SDIFF";
    for (const auto& key : keys) {
        cmd << " " << key;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> diffSet;
        for (size_t i = 0; i < reply->elements; ++i) {
            auto elem = reply->element[i];
            if (elem->type == REDIS_REPLY_STRING) {
                diffSet.push_back(std::string(elem->str, elem->len));
            } else {
                throw std::runtime_error("Invalid entry in SDIFF reply");
            }
        }
        return diffSet;
    } else {
        throw std::runtime_error("Unexpected reply when executing SDIFF for keys");
    }
}
bool RedisClient::sdiffstore(const std::string& destination, const std::vector<std::string>& keys) {
    std::stringstream cmd;
    cmd << "SDIFFSTORE " << destination;
    for (const auto& key : keys) {
        cmd << " " << key;
    }
    
    auto reply = Command(cmd.str().c_str());
    return reply && reply->type == REDIS_REPLY_INTEGER && reply->integer >= 0;
}
std::vector<std::string> RedisClient::sinter(const std::vector<std::string>& keys) {
    std::stringstream cmd;
    cmd << "SINTER";
    for (const auto& key : keys) {
        cmd << " " << key;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> intersection;
        for (size_t i = 0; i < reply->elements; ++i) {
            auto elem = reply->element[i];
            if (elem->type == REDIS_REPLY_STRING) {
                intersection.push_back(std::string(elem->str, elem->len));
            } else {
                throw std::runtime_error("Invalid entry in SINTER reply");
            }
        }
        return intersection;
    } else {
        throw std::runtime_error("Unexpected reply when executing SINTER for keys");
    }
}
bool RedisClient::sinterstore(const std::string& destination, const std::vector<std::string>& keys) {
    std::stringstream cmd;
    cmd << "SINTERSTORE " << destination;
    for (const auto& key : keys) {
        cmd << " " << key;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer >= 0;
    } else {
        throw std::runtime_error("Unexpected reply when executing SINTERSTORE for destination: " + destination);
    }
}
std::vector<std::string> RedisClient::smembers(const std::string& key) {
    auto reply = Command("SMEMBERS %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
        throw std::runtime_error("Unexpected reply when executing SMEMBERS for key: " + key);
    }
    
    std::vector<std::string> members;
    for (size_t i = 0; i < reply->elements; ++i) {
        auto elem = reply->element[i];
        if (elem->type == REDIS_REPLY_STRING) {
            members.push_back(std::string(elem->str, elem->len));
        } else {
            throw std::runtime_error("Invalid entry in SMEMBERS reply for key: " + key);
        }
    }
    return members;
}
bool RedisClient::smove(const std::string& source, const std::string& destination, const std::string& member) {
    auto reply = Command("SMOVE %s %s %s", source.c_str(), destination.c_str(), member.c_str());
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        throw std::runtime_error("Unexpected reply when executing SMOVE from " + source + " to " + destination);
    }
    
    return reply->integer == 1;
}
bool RedisClient::sismember(const std::string& key, const std::string& member) {
    auto reply = Command("SISMEMBER %s %s", key.c_str(), member.c_str());
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        throw std::runtime_error("Unexpected reply when executing SISMEMBER for key: " + key);
    }
    return reply->integer == 1;
}
std::optional<std::string> RedisClient::spop(const std::string& key) {
    auto reply = Command("SPOP %s", key.c_str());
    if (reply && reply->type == REDIS_REPLY_NIL) {
        return std::nullopt;
    } else if (reply && reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len);
    } else {
        throw std::runtime_error("Unexpected reply when executing SPOP for key: " + key);
    }
}
std::vector<std::string> RedisClient::srandmember(const std::string& key, size_t count) {
    std::stringstream cmd;
    cmd << "SRANDMEMBER " << key << " " << count;
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> randomMembers;
        for (size_t i = 0; i < reply->elements; ++i) {
            auto elem = reply->element[i];
            if (elem->type == REDIS_REPLY_STRING) {
                randomMembers.push_back(std::string(elem->str, elem->len));
            } else {
                throw std::runtime_error("Invalid entry in SRANDMEMBER reply");
            }
        }
        return randomMembers;
    } else {
        throw std::runtime_error("Unexpected reply when executing SRANDMEMBER for key: " + key);
    }
}
bool RedisClient::srem(const std::string& key, const std::vector<std::string>& members, int& removedCount) {
    std::stringstream cmd;
    cmd << "SREM " << key;
    for (const auto& member : members) {
        cmd << " " << member;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        removedCount = reply->integer;
        return true;
    } else {
        throw std::runtime_error("Unexpected reply when executing SREM for key: " + key);
    }
}
std::vector<std::string> RedisClient::sunion(const std::vector<std::string>& keys) {
    std::stringstream cmd;
    cmd << "SUNION";
    for (const auto& key : keys) {
        cmd << " " << key;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> unionSet;
        for (size_t i = 0; i < reply->elements; ++i) {
            auto elem = reply->element[i];
            if (elem->type == REDIS_REPLY_STRING) {
                unionSet.push_back(std::string(elem->str, elem->len));
            } else {
                throw std::runtime_error("Invalid entry in SUNION reply");
            }
        }
        return unionSet;
    } else {
        throw std::runtime_error("Unexpected reply when executing SUNION for keys");
    }
}
bool RedisClient::sunionstore(const std::string& destination, const std::vector<std::string>& keys) {
    std::stringstream cmd;
    cmd << "SUNIONSTORE " << destination;
    for (const auto& key : keys) {
        cmd << " " << key;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer >= 0;
    } else {
        throw std::runtime_error("Unexpected reply when executing SUNIONSTORE for destination: " + destination);
    }
}
bool RedisClient::zadd(const std::string& key, const std::vector<std::pair<std::string, double>>& membersWithScores, int& addedCount) {
    std::stringstream cmd;
    cmd << "ZADD " << key;
    for (const auto& pair : membersWithScores) {
        cmd << " " << pair.second << " " << pair.first;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        addedCount = reply->integer;
        return true;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZADD for key: " + key);
    }
}
int64_t RedisClient::zcard(const std::string& key) {
    auto reply = Command("ZCARD %s", key.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZCARD for key: " + key);
    }
}
int64_t RedisClient::zcount(const std::string& key, double minScore, double maxScore) {
    auto reply = Command("ZCOUNT %s %f %f", key.c_str(), minScore, maxScore);
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZCOUNT for key: " + key);
    }
}
double RedisClient::zincrby(const std::string& key, double increment, const std::string& member) {
    auto reply = Command("ZINCRBY %s %f %s", key.c_str(), increment, member.c_str());
    if (reply && reply->type == REDIS_REPLY_STRING) {
        return std::stod(reply->str);
    } else {
        throw std::runtime_error("Unexpected reply when executing ZINCRBY for key: " + key);
    }
}
bool RedisClient::zinterstore(const std::string& destination, const std::vector<std::string>& keys, const std::vector<std::string>& weights /* optional */, bool aggregateSum /* default */, int64_t& count) {
    std::stringstream cmd;
    cmd << "ZINTERSTORE " << destination << " " << keys.size();
    for (const auto& key : keys) {
        cmd << " " << key;
    }
    if (!weights.empty()) {
        cmd << " WEIGHTS";
        for (const auto& weight : weights) {
            cmd << " " << weight;
        }
    }
    cmd << " " << (aggregateSum ? "SUM" : "AGGREGATE");

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        count = reply->integer;
        return true;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZINTERSTORE for destination: " + destination);
    }
}
int64_t RedisClient::zlexcount(const std::string& key, const std::string& minMember, const std::string& maxMember) {
    auto reply = Command("ZLEXCOUNT %s %s %s", key.c_str(), minMember.c_str(), maxMember.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZLEXCOUNT for key: " + key);
    }
}
std::vector<std::string> RedisClient::zrange(const std::string& key, int start, int stop, bool withScores) {
    std::stringstream cmd;
    cmd << "ZRANGE " << key << " " << start << " " << stop;
    if (withScores) {
        cmd << " WITHSCORES";
    }

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> members;
        for (size_t i = 0; i < reply->elements; ++i) {
            auto elem = reply->element[i];
            if (elem->type == REDIS_REPLY_STRING) {
                members.push_back(std::string(elem->str, elem->len));
            } else if (withScores && elem->type == REDIS_REPLY_STRING) { 
                members.push_back(elem->str); 
            } else {
                throw std::runtime_error("Invalid entry in ZRANGE reply");
            }
        }
        return members;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZRANGE for key: " + key);
    }
}
std::vector<std::string> RedisClient::zrangebylex(const std::string& key, const std::string& minLex, const std::string& maxLex, bool withScores, long long offset, long long count) {
    std::stringstream cmd;
    cmd << "ZRANGEBYLEX " << key << " " << minLex << " " << maxLex;
    if (withScores) {
        cmd << " WITHSCORES";
    }
    if (offset > 0 || count > 0) {
        cmd << " LIMIT " << offset << " " << count;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> results;
        for (size_t i = 0; i < reply->elements; ++i) {
            auto elem = reply->element[i];
            if (elem->type == REDIS_REPLY_STRING) {
                results.push_back(std::string(elem->str, elem->len));
            } else if (withScores && elem->type == REDIS_REPLY_STRING) {
                results.push_back(elem->str); 
            } else {
                throw std::runtime_error("Unexpected element type in ZRANGEBYLEX reply.");
            }
        }
        return results;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZRANGEBYLEX for key: " + key);
    }
}
std::vector<std::string> RedisClient::zrangebyscore(const std::string& key, double minScore, double maxScore, bool withScores, bool reverse, long long limitOffset, long long limitCount) {
    std::stringstream cmd;
    cmd << "ZRANGEBYSCORE " << key << " " << minScore << " " << maxScore;
    if (withScores) {
        cmd << " WITHSCORES";
    }
    if (reverse) {
        cmd << " REV";
    }
    if (limitOffset >= 0 && limitCount > 0) {
        cmd << " LIMIT " << limitOffset << " " << limitCount;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> results;
        for (size_t i = 0; i < reply->elements; ++i) {
            auto elem = reply->element[i];
            if (elem->type == REDIS_REPLY_STRING) {
                results.push_back(std::string(elem->str, elem->len));
            } else if (withScores && elem->type == REDIS_REPLY_STRING) {
                results.push_back(elem->str);
            } else {
                throw std::runtime_error("Unexpected element type in ZRANGEBYSCORE reply.");
            }
        }
        return results;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZRANGEBYSCORE for key: " + key);
    }
}
int64_t RedisClient::zrank(const std::string& key, const std::string& member) {
    auto reply = Command("ZRANK %s %s", key.c_str(), member.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else if (reply->type == REDIS_REPLY_NIL) {
        return -1;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZRANK for key: " + key);
    }
}
int64_t RedisClient::zremrangebylex(const std::string& key, const std::string& minLex, const std::string& maxLex) {
    auto reply = Command("ZREMRANGEBYLEX %s %s %s", key.c_str(), minLex.c_str(), maxLex.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZREMRANGEBYLEX for key: " + key);
    }
}
int64_t RedisClient::zremrangebyrank(const std::string& key, int start, int stop) {
    std::stringstream cmd;
    cmd << "ZREMRANGEBYRANK " << key << " " << start << " " << stop;

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer; 
    } else {
        throw std::runtime_error("Unexpected reply when executing ZREMRANGEBYRANK for key: " + key);
    }
}
int64_t RedisClient::zremrangebyscore(const std::string& key, double minScore, double maxScore) {
    std::stringstream cmd;
    cmd << "ZREMRANGEBYSCORE " << key << " " << minScore << " " << maxScore;

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZREMRANGEBYSCORE for key: " + key);
    }
}
std::vector<std::string> RedisClient::zrevrange(const std::string& key, int start, int stop, bool withscores) {
    std::stringstream cmd;
    cmd << "ZREVRANGE " << key << " " << start << " " << stop;
    if (withscores) {
        cmd << " WITHSCORES";
    }

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> elements;
        for (size_t i = 0; i < reply->elements; ++i) {
            auto element = reply->element[i];
            if (element->type != REDIS_REPLY_STRING) {
                throw std::runtime_error("Unexpected element type in ZREVRANGE reply.");
            }
            elements.push_back(std::string(element->str, element->len));
        }
        return elements;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZREVRANGE for key: " + key);
    }
}
std::vector<std::string> RedisClient::zrevrangebyscore(const std::string& key, double maxScore, double minScore, bool withScores, int offset, int count) {
    std::stringstream cmd;
    cmd << "ZREVRANGEBYSCORE " << key << " " << maxScore << " " << minScore;
    if (withScores) {
        cmd << " WITHSCORES";
    }
    if (offset > 0) {
        cmd << " OFFSET " << offset;
    }
    if (count > 0) {
        cmd << " COUNT " << count;
    }
    
    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> results;
        for (size_t i = 0; i < reply->elements; ++i) {
            auto elem = reply->element[i];
            if (elem->type == REDIS_REPLY_STRING) {
                results.push_back(std::string(elem->str, elem->len));
            } else if (withScores && elem->type == REDIS_REPLY_STRING) {
                results.push_back(elem->str);
            } else {
                throw std::runtime_error("Unexpected element type in ZREVRANGEBYSCORE reply.");
            }
        }
        return results;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZREVRANGEBYSCORE for key: " + key);
    }
}
int64_t RedisClient::zrevrank(const std::string& key, const std::string& member) {
    auto reply = Command("ZREVRANK %s %s", key.c_str(), member.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else if (reply->type == REDIS_REPLY_NIL) {
        return -1;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZREVRANK for key: " + key);
    }
}
std::optional<double> RedisClient::zscore(const std::string& key, const std::string& member) {
    auto reply = Command("ZSCORE %s %s", key.c_str(), member.c_str());
    if (reply && reply->type == REDIS_REPLY_STRING) {
        return std::stod(reply->str);
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        return std::nullopt;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZSCORE for key: " + key);
    }
}
bool RedisClient::zunionstore(const std::string& destination, const std::vector<std::string>& keys, const std::vector<double>& weights /* = {} */, const std::string& aggregate /* = "SUM" */) {
    std::stringstream cmd;
    cmd << "ZUNIONSTORE " << destination << " " << keys.size();
    for (const auto& key : keys) {
        cmd << " " << key;
    }
    if (!weights.empty()) {
        cmd << " WEIGHTS";
        for (const auto& weight : weights) {
            cmd << " " << weight;
        }
    }
    if (!aggregate.empty() && aggregate != "SUM") {
        cmd << " " << aggregate; 
    }

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer >= 0;
    } else {
        throw std::runtime_error("Unexpected reply when executing ZUNIONSTORE for destination: " + destination);
    }
}
std::pair<std::string, std::string> RedisClient::blpop(const std::vector<std::string>& keys, int timeout) {
    std::stringstream cmd;
    cmd << "BLPOP ";
    for (size_t i = 0; i < keys.size(); ++i) {
        cmd << keys[i] << (i < keys.size() - 1 ? " " : "");
    }
    cmd << timeout;

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements == 2) {
        auto keyReply = reply->element[0];
        auto valueReply = reply->element[1];
        if (keyReply->type == REDIS_REPLY_STRING && valueReply->type == REDIS_REPLY_STRING) {
            return {std::string(keyReply->str, keyReply->len), std::string(valueReply->str, valueReply->len)};
        }
    } else {
        throw std::runtime_error("Unexpected reply when executing BLPOP for keys");
    }
}
std::pair<std::string, std::string> RedisClient::brpop(const std::vector<std::string>& keys, int timeout) {
    std::stringstream cmd;
    cmd << "BRPOP ";
    for (size_t i = 0; i < keys.size(); ++i) {
        cmd << keys[i] << (i < keys.size() - 1 ? " " : "");
    }
    cmd << timeout;

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_ARRAY && reply->elements == 2) {
        auto keyReply = reply->element[0];
        auto valueReply = reply->element[1];
        if (keyReply->type == REDIS_REPLY_STRING && valueReply->type == REDIS_REPLY_STRING) {
            return {std::string(keyReply->str, keyReply->len), std::string(valueReply->str, valueReply->len)};
        }
    } else {
        throw std::runtime_error("Unexpected reply when executing BRPOP for keys");
    }
}
std::string RedisClient::brpoplpush(const std::string& source, const std::string& destination, int timeout) {
    std::stringstream cmd;
    cmd << "BRPOPLPUSH " << source << " " << destination << " " << timeout;

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len); 
    } else {
        throw std::runtime_error("Unexpected reply when executing BRPOPLPUSH from " + source + " to " + destination);
    }
}
std::optional<std::string> RedisClient::lindex(const std::string& key, long long index) {
    auto reply = Command("LINDEX %s %lld", key.c_str(), index);
    if (reply && reply->type == REDIS_REPLY_NIL) {
        return std::nullopt;
    } else if (reply && reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len);
    } else {
        throw std::runtime_error("Unexpected reply when executing LINDEX for key: " + key);
    }
}
bool RedisClient::linsert(const std::string& key, const std::string& pivot, const std::string& value, bool before) {
    std::string position = before ? "BEFORE" : "AFTER";
    auto reply = Command("LINSERT %s %s %s %s %s", key.c_str(), position.c_str(), pivot.c_str(), value.c_str());

    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer >= 0; 
    } else {
        throw std::runtime_error("Unexpected reply when executing LINSERT for key: " + key);
    }
}
size_t RedisClient::llen(const std::string& key) {
    auto reply = Command("LLEN %s", key.c_str());

    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return static_cast<size_t>(reply->integer);
    } else {
        throw std::runtime_error("Unexpected reply when executing LLEN for key: " + key);
    }
}
std::optional<std::string> RedisClient::lpop(const std::string& key) {
    auto reply = Command("LPOP %s", key.c_str());
    if (reply && reply->type == REDIS_REPLY_NIL) {
        return std::nullopt;
    } else if (reply && reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len);
    } else {
        throw std::runtime_error("Unexpected reply when executing LPOP for key: " + key);
    }
}
long long RedisClient::lpush(const std::string& key, const std::vector<std::string>& values) {
    std::stringstream cmd;
    cmd << "LPUSH " << key;
    for (const auto& value : values) {
        cmd << " " << value;
    }

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply when executing LPUSH for key: " + key);
    }
}
long long RedisClient::lpushx(const std::string& key, const std::string& value) {
    auto reply = Command("LPUSHX %s %s", key.c_str(), value.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        return -1; 
    } else {
        throw std::runtime_error("Unexpected reply when executing LPUSHX for key: " + key);
    }
}
std::vector<std::string> RedisClient::lrange(const std::string& key, long long start, long long stop) {
    std::string command = "LRANGE " + key + " " + std::to_string(start) + " " + std::to_string(stop);
    auto reply = Command(command.c_str());

    if (reply && reply->type == REDIS_REPLY_ARRAY) {
        std::vector<std::string> elements;
        for (size_t i = 0; i < reply->elements; ++i) {
            if (reply->element[i]->type == REDIS_REPLY_STRING) {
                elements.push_back(std::string(reply->element[i]->str, reply->element[i]->len));
            }
        }
        return elements;
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        return {}; 
    } else {
        throw std::runtime_error("Unexpected reply type when executing LRANGE for key: " + key);
    }
}
int64_t RedisClient::lrem(const std::string& key, int count, const std::string& value) {
    auto reply = Command("LREM %s %d %s", key.c_str(), count, value.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer;
    } else {
        throw std::runtime_error("Unexpected reply when executing LREM for key: " + key);
    }
}
bool RedisClient::lset(const std::string& key, long long index, const std::string& value) {
    auto reply = Command("LSET %s %lld %s %s", key.c_str(), index, value.c_str());
    if (reply && reply->type == REDIS_REPLY_STATUS) {
        return std::strcmp(reply->str, "OK") == 0;
    } else {
        throw std::runtime_error("Unexpected reply when executing LSET for key: " + key);
    }
}
bool RedisClient::ltrim(const std::string& key, long long start, long long stop) {
    auto reply = Command("LTRIM %s %lld %lld", key.c_str(), start, stop);
    if (reply && reply->type == REDIS_REPLY_STATUS) {
        return std::strcmp(reply->str, "OK") == 0;
    } else {
        throw std::runtime_error("Unexpected reply when executing LTRIM for key: " + key);
    }
}
std::optional<std::string> RedisClient::rpop(const std::string& key) {
    auto reply = Command("RPOP %s", key.c_str());
    if (reply && reply->type == REDIS_REPLY_NIL) {
        return std::nullopt;
    } else if (reply && reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len);
    } else {
        throw std::runtime_error("Unexpected reply when executing RPOP for key: " + key);
    }
}
std::string RedisClient::rpoplpush(const std::string& source, const std::string& destination) {
    auto reply = Command("RPOPLPUSH %s %s %s", source.c_str(), destination.c_str());
    if (reply && reply->type == REDIS_REPLY_STRING) {
        return std::string(reply->str, reply->len);
    } else {
        throw std::runtime_error("Unexpected reply when executing RPOPLPUSH from " + source + " to " + destination);
    }
}
long long RedisClient::rpush(const std::string& key, const std::vector<std::string>& values) {
    std::stringstream cmd;
    cmd << "RPUSH " << key;
    for (const auto& value : values) {
        cmd << " " << value;
    }

    auto reply = Command(cmd.str().c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer; 
    } else {
        throw std::runtime_error("Unexpected reply when executing RPUSH for key: " + key);
    }
}
long long RedisClient::rpushx(const std::string& key, const std::string& value) {
    auto reply = Command("RPUSHX %s %s", key.c_str(), value.c_str());
    if (reply && reply->type == REDIS_REPLY_INTEGER) {
        return reply->integer; 
    } else if (reply && reply->type == REDIS_REPLY_NIL) {
        return -1; 
    } else {
        throw std::runtime_error("Unexpected reply when executing RPUSHX for key: " + key);
    }
}