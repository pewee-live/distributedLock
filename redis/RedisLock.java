package redis.distributedlock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.commons.io.IOUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * 分布式锁,基于redis
 * @author pewee
 *
 */
public class RedisLock {
	
	public static final String LOCK_NAME = "RedisLock_RefreshToken";
	private static final Long RELEASE_SUCCESS = 1L;
	
	static JedisPool jedisPool;
	
	static String luasrc;
	
	static {
		String redisIp = "127.0.0.1";
        int reidsPort = 6379;
        jedisPool = new JedisPool(new JedisPoolConfig(), redisIp, reidsPort);
        Jedis jedis = jedisPool.getResource();
		System.out.println(jedis.ping());
		jedis.close();
		
		try {
			luasrc =IOUtils.toString(RedisLock.class.getClassLoader().getResourceAsStream("redislock.lua"), 
					"UTF-8") ;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Jedis jedis = jedisPool.getResource();
		String requestId =Thread.currentThread().getName() + "-" + Thread.currentThread().getId();
		boolean b = tryLock(jedis, LOCK_NAME, requestId, 30000);
		if(b) {
			System.out.println("主线程成功获取锁");
		} else {
			System.out.println("主线程没有成功获取锁");
		}
		System.out.println("主线程业务处理完成,开始释放锁!");
        boolean releaseDistributedLock = RedisLock.releaseLock(jedis, LOCK_NAME, requestId);
        if(releaseDistributedLock) {
        	 System.out.println("主线程释放锁成功");
        } else {
        	System.out.println("主线程释放锁失败");
        }
	}
	
	/**
     * 尝试获取分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 请求标识
     * @param expireTime 锁超期时间,毫秒
     * @return 是否获取成功
     */
	public static boolean tryLock(Jedis jedis, String lockKey, String requestId, int expireTime) {
		ArrayList<String> keys = new ArrayList<String>();
		keys.add("lock");
		keys.add(lockKey);
		Object result = jedis.eval(luasrc, keys, Collections.singletonList(requestId));
		System.out.println("返回:" + result);
        if (RELEASE_SUCCESS.equals(result)) {
            return true;
        }
        return false;
		
    }
	
	/**
     * 释放分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 请求标识
     * @return 是否释放成功
     */
    public static boolean releaseLock(Jedis jedis, String lockKey, String requestId) {
    	ArrayList<String> keys = new ArrayList<String>();
		keys.add("unlock");
		keys.add(lockKey);
    	Object result = jedis.eval(luasrc, keys, Collections.singletonList(requestId));
    	System.out.println("返回:" + result);
    	if (RELEASE_SUCCESS.equals(result)) {
            return true;
        }
        return false;

    }
	
	
}
