package io.larkin.tate2neo.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RedisLookupRepository implements ILookupRepository {
	
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	
	@Override
	public void add(String key, String value) {
		redisTemplate.boundValueOps(key).append(value);
	}

	@Override
	public String get(String key) {
		return redisTemplate.opsForValue().get(key);
	}

}
