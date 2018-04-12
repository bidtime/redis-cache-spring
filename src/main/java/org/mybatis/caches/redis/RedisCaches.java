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
 * 项目地址：https://github.com/mybatis/redis-cache.git
 * 
 * Cache adapter for Redis.
 *
 * @author riverbo
 */
public final class RedisCaches implements Cache {

  private static final Logger log = LoggerFactory.getLogger(RedisCaches.class);

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private String id;

  private RedisConnectionFactory factory;

  private boolean cluster = false;

  private Integer timeout;

  public RedisCaches(final String id) {
    if (id == null) {
      throw new IllegalArgumentException("Cache instances require an ID");
    }
    this.id = id;
  }

  // TODO Review this is UNUSED
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

      @Override
      public Object doInRedis(RedisConnection conn) {
        final byte[] idBytes = id.getBytes();
        conn.hSet(idBytes, key.toString().getBytes(), SerializeUtil.serialize(value));
        if (timeout != null && conn.ttl(idBytes) == -1) {
          conn.expire(idBytes, timeout);
        }
        return null;
      }
    });
  }

  @Override
  public Object getObject(final Object key) {
    return execute(new RedisCallback<Object>() {

      @Override
      public Object doInRedis(RedisConnection conn) {
        byte[] result = conn.hGet(id.getBytes(), key.toString().getBytes());
        return SerializeUtil.unserialize(result);
      }
    });
  }

  @Override
  public Object removeObject(final Object key) {
    return execute(new RedisCallback<Long>() {

      @Override
      public Long doInRedis(RedisConnection conn) {
        return conn.hDel(id.getBytes(), key.toString().getBytes());
      }
    });
  }

  @Override
  public void clear() {
    execute(new RedisCallback<Object>() {

      @Override
      public Object doInRedis(RedisConnection conn) {
        conn.del(id.getBytes());
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

  public void setFactory(RedisConnectionFactory factory) {
    this.factory = factory;
  }

  public boolean isCluster() {
    return cluster;
  }

  public void setCluster(boolean cluster) {
    this.cluster = cluster;
  }

  public Integer getTimeout() {
    return timeout;
  }

}
