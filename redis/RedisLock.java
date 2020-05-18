package com.pewee.test10.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * 分布式锁,基于redis
 * @author pewee
 *
 */
@Component
public class RedisLock {
	
	static public Logger logger = LoggerFactory.getLogger(RedisLock.class);
	
	public static final String LOCK_NAME = "RedisLock_RefreshToken";
	private static final Long FAIL = 0L;
	private static final Long SUCCESS = 1L;
	private static final Long WRONG_OPA = 3L;
	
	private static final String LOCK = "lock";
	
	private static final String UNLOCK = "unlock";
	
	static JedisCluster jedisCluster ;
	
	static String luasrc;
	
	private static Pattern p = Pattern.compile("^.+[:]\\d{1,5}\\s*$");  
	
	static {
		JedisPoolConfig poolConfig = new redis.clients.jedis.JedisPoolConfig();
		poolConfig.setMaxIdle(1000);
		poolConfig.setMaxWaitMillis(1000);
		poolConfig.setTestOnBorrow(true);
		poolConfig.setMaxTotal(1000);
		poolConfig.setMinIdle(8);
		
		String hostAndPort = "127.0.0.1:6380";
		String[] hostAndPortsArr  = hostAndPort.split(";");
        Set<HostAndPort> haps = new HashSet<HostAndPort>();  
        for (String hostport : hostAndPortsArr) {  
            boolean isIpPort = p.matcher(hostport).matches();  
            if (!isIpPort) {  
                throw new IllegalArgumentException("ip 或 port 不合法");  
            }  
            String[] ipAndPort = hostport.split(":");  
            HostAndPort hap = new HostAndPort(ipAndPort[0], Integer.parseInt(ipAndPort[1]));  
            haps.add(hap);  
        }   
		jedisCluster = new JedisCluster(haps, 3000, 1000,3,"Pass@word",poolConfig);
		try {
			luasrc =IOUtils.toString(RedisLock.class.getClassLoader().getResourceAsStream("redislock.lua"), 
					"UTF-8") ;
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(keys(jedisCluster, "*"));
		System.out.println(jedisCluster.get("{RedisLock_RefreshToken}RedisLock_RefreshToken"));
	}
	
	public static void main(String[] args) {
		//String requestId =Thread.currentThread().getName() + "-" + Thread.currentThread().getId();
		String requestId =UUID.randomUUID().toString();
		boolean b = tryLock(jedisCluster, LOCK_NAME, requestId, 5000);
		if(b) {
			System.out.println("主线程成功获取锁");
			System.out.println("主线程业务处理完成,开始释放锁!");
	        boolean releaseDistributedLock = RedisLock.releaseLock(jedisCluster, LOCK_NAME, requestId, 5000);
	        if(releaseDistributedLock) {
	        	 System.out.println("主线程释放锁成功");
	        } else {
	        	System.out.println("主线程释放锁失败");
	        }
		} else {
			System.out.println("主线程没有成功获取锁");
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
	public static boolean tryLock(JedisCluster jedisCluster, String lockKey, String requestId, int expireTime) {
		long start = System.currentTimeMillis();
		long end = start + expireTime;
		boolean flag = false;
		while(System.currentTimeMillis() <= end && !flag) {
			flag = doTryLock(jedisCluster, lockKey, requestId);
			if(flag) {
				return flag;
			} else {
				try {
					Thread.currentThread().sleep(100L);
				} catch (InterruptedException e) {
					logger.error("线程被打断",e);
				}	
			}
		}
		return flag;
    }
	
	private static synchronized boolean doTryLock(JedisCluster jedisCluster, String lockKey, String requestId) {
		ArrayList<String> keys = new ArrayList<String>();
		keys.add(getHashKey(lockKey) + LOCK);
		keys.add(getHashKey(lockKey) + lockKey);
		Object result = jedisCluster.eval(luasrc, keys, Collections.singletonList(requestId));
		System.out.println("返回:" + result);
        if (SUCCESS.equals(result)) {
            return true;
        }
        return false;
    }
	
	
	/**
     * 释放分布式锁
     * @param jedis Redis客户端
     * @param lockKey 锁
     * @param requestId 请求标识
     * @param expireTime 锁超期时间,毫秒
     * @return 是否释放成功
     */
    public static  boolean releaseLock(JedisCluster jedisCluster, String lockKey, String requestId, int expireTime) {
    	long start = System.currentTimeMillis();
		long end = start + expireTime;
		boolean flag = false;
		while(System.currentTimeMillis() <= end && !flag) {
			flag = doReleaseLock(jedisCluster, lockKey, requestId);
			if(flag) {
				return flag;
			} else {
				try {
					Thread.currentThread().sleep(100L);
				} catch (InterruptedException e) {
					logger.error("线程被打断",e);
				}	
			}
		}
		return flag;
    }
    
    public static synchronized boolean doReleaseLock(JedisCluster jedisCluster, String lockKey, String requestId) {
    	ArrayList<String> keys = new ArrayList<String>();
		keys.add(getHashKey(lockKey) + UNLOCK);
		keys.add(getHashKey(lockKey) + lockKey);
    	Object result = jedisCluster.eval(luasrc, keys, Collections.singletonList(requestId));
    	System.out.println("返回:" + result);
    	if (SUCCESS.equals(result)) {
            return true;
        }
        return false;
    }
	
	private static String getHashKey(String lockKey) {
		return "{" + lockKey + "}";
	}
	
	public static Set<String> keys(JedisCluster jedisCluster,String pattern) {
		 
        logger.debug("Start getting keys... ");
        TreeSet<String> keys = new TreeSet<>();
        Map<String, JedisPool> clusterNodes =
                jedisCluster.getClusterNodes();
 
        for (String key : clusterNodes.keySet()) {
            logger.debug("Getting keys from: {}", key);
            JedisPool jedisPool = clusterNodes.get(key);
            Jedis jedisConn = jedisPool.getResource();
            try {
                keys.addAll(jedisConn.keys(pattern));
            } catch (Exception e) {
                logger.error("Getting keys error: {}", e);
            } finally {
                logger.debug("Jedis connection closed");
                jedisConn.close();
            }
        }
        logger.debug("Keys gotten");
 
        return keys;
    }

}
