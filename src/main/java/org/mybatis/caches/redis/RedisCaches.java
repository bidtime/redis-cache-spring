/**
 *    Copyright 2015-2018 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.mybatis.caches.redis;

import java.util.Collection;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.ibatis.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;

/**
 * 本源码摘自 mybatis-redis 项目，并加以改造
 * 
 * 项目 mybatis-redis 地址：https://github.com/mybatis/redis-cache.git
 * 
 * Cache adapter for Redis.
 *
 * @author riverbo
 */
public final class RedisCaches implements Cache {

  private static final Logger log = LoggerFactory.getLogger(RedisCaches.class);

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private String id;
  
  private byte[] idBytes;

  /**
   * 与 spring-data-redis 的 RedisConnectionFactory，结合使用
   */
  private static RedisConnectionFactory factory;

  /*
   * redis 服务器 是否使用集群
   */
  private static boolean cluster = false;

  private Integer timeout;

  public RedisCaches(final String id) {
    if (id == null) {
      throw new IllegalArgumentException("Cache instances require an ID");
    }
    this.id = id;
    this.idBytes = id.getBytes();
    if (log.isDebugEnabled()) {
      log.debug("cache: {}", id);
    }
  }

  private <T> T execute(RedisCallback<T> callback) {
    RedisConnection conn = null;
    try {
      if (!cluster) {
        conn = factory.getConnection();
      } else {
        conn = factory.getClusterConnection();
      }
    } catch (Exception e) {
      log.error("get connection: {}, {}", e.getMessage(), e.getStackTrace());
      return null;
    }
    
    if (conn == null) {
      return null;
    }

    try {
      return (T) callback.doInRedis(conn);
    } catch (Exception e) {
      log.error("exceute: {}, {}", e.getMessage(), e.getStackTrace());
      return null;
    } finally {
      conn.close();
    }
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public int getSize() {
    return (Integer) execute(new RedisCallback<Integer>() {

      @Override
      public Integer doInRedis(RedisConnection conn) {
        return conn.dbSize().intValue();
      }
    });
  }

  @Override
  public void putObject(final Object key, final Object value) {
    execute(new RedisCallback<Object>() {

      @SuppressWarnings("rawtypes")
      @Override
      public Object doInRedis(RedisConnection conn) {
        Boolean success = null;
        final byte[] keyBytes = key.toString().getBytes();
        byte[] dataBytes = null;
        if (value != null) {
          dataBytes = SerializeUtil.serialize(value);
        }
        success = conn.hSet(idBytes, keyBytes, dataBytes);
        if (timeout != null && conn.ttl(idBytes) == -1) {
          conn.expire(idBytes, timeout);
        }
        if (log.isDebugEnabled()) {
          if (value != null) {
            if (value instanceof Collection) {
              log.debug("put: {}-{}, {}:size({})", id, key, success, ((Collection)value).size());
            } else {
              log.debug("put: {}-{}, {}", id, key, success);
            }
          } else {
            log.debug("put: {}-{}, {}", id, key, success);
          }
        }
        return null;
      }
    });
  }

  @Override
  public Object getObject(final Object key) {
    return execute(new RedisCallback<Object>() {

      @SuppressWarnings("rawtypes")
      @Override
      public Object doInRedis(RedisConnection conn) {
        final byte[] keyBytes = key.toString().getBytes();
        Object result = null;
        final byte[] dataBytes = conn.hGet(idBytes, keyBytes);
        if (dataBytes != null) {
          result = SerializeUtil.unserialize(dataBytes);
        }
        if (log.isDebugEnabled()) {
          log.debug("get: {}-{}, {}", id, key, (result == null) ? null : result.getClass().getName());
        }
        if (log.isDebugEnabled()) {
          if (result != null) {
            if (result instanceof Collection) {
              log.debug("get: {}-{}, {}:size({})", id, key, result.getClass().getName(), ((Collection)result).size());
            } else {
              log.debug("get: {}-{}, {}", id, key, result.getClass().getName());
            }
          } else {
            log.debug("get: {}-{}, {}: is null", id, key, (result == null) ? null : result.getClass().getName());
          }
        }
        return result;
      }
    });
  }

  @Override
  public Object removeObject(final Object key) {
    return execute(new RedisCallback<Long>() {

      @Override
      public Long doInRedis(RedisConnection conn) {
        final byte[] keyBytes = key.toString().getBytes();
        Long l = conn.hDel(idBytes, keyBytes);
        if (log.isDebugEnabled()) {
          log.debug("del: {}-{}, {}", id, key, (l == null) ? null : l);
        }        
        return l;
      }
    });
  }

  @Override
  public void clear() {
    execute(new RedisCallback<Object>() {

      @Override
      public Object doInRedis(RedisConnection conn) {
        if (conn == null) {
          return null;
        }
        Long l = conn.del(idBytes);
        if (log.isDebugEnabled()) {
          log.debug("clear: {}, {}", id, (l == null) ? null : l);
        }        
        return null;
      }
    });

  }

  @Override
  public ReadWriteLock getReadWriteLock() {
    return readWriteLock;
  }

  @Override
  public String toString() {
    return "Redis {" + id + "}";
  }

  // properties

  public void setTimeout(Integer timeout) {
    this.timeout = timeout;
  }

  public RedisConnectionFactory getFactory() {
    return factory;
  }

  @SuppressWarnings("static-access")
  public void setFactory(RedisConnectionFactory factory) {
    this.factory = factory;
  }

  public boolean isCluster() {
    return cluster;
  }

  public static void setCluster(boolean cluster) {
    log.debug("set cluster: {}", cluster);
    RedisCaches.cluster = cluster;
  }

  public Integer getTimeout() {
    return timeout;
  }
  
  public static void setRedisConnectionFactory(RedisConnectionFactory factory) {
    log.debug("factory: {}", factory.getClass().getName());
    RedisCaches.factory = factory;
  }
  
}