package org.mybatis.caches.redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnectionFactory;

public class RedisCacheTransfer {

  @Autowired
  public void setRedisConnectionFactory(RedisConnectionFactory factory) {
    RedisCaches.setRedisConnectionFactory(factory);
  }

  @Autowired
  public void setCluster(boolean cluster) {
    RedisCaches.setCluster(cluster);
  }

}
