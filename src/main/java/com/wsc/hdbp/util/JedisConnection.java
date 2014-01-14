/**
 *
 */
package com.wsc.hdbp.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Elliot
 */
public class JedisConnection {
	private static Properties properties;
	private static JedisPool pool = null;
	private static JedisPoolConfig config = new JedisPoolConfig();

	public static void initPool() {
		try {
			properties = new Properties();
			properties.load(JedisConnection.class.getClassLoader()
					.getResourceAsStream("redis.properties"));

			config.setMaxActive(100);
			config.setMaxIdle(20);
			config.setMaxWait(1000l);

			pool = new JedisPool(config,
					properties.getProperty("redis.server.host"),
					Integer.parseInt(properties.getProperty(
							"redis.server.port", "6379")), 100000);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * static method to retrieve Jedis database connection with a specific
	 * databse index number.
	 * 
	 * @param index
	 *            database number
	 * @return Jedis connection
	 */
	public static Jedis getJedisConnection() {
		if (pool == null) {
			initPool();
		}
		Jedis jedis = pool.getResource();
		return jedis;
	}

	public static void dropJedisConnection(Jedis jedis) {
		pool.returnResource(jedis);
	}

	public static void dropBrokenJedisConnection(Jedis jedis) {
		pool.returnBrokenResource(jedis);
	}

	/**
	 * The Constructor JedisConnection. Set private to prevent instantiation.
	 */
	private JedisConnection() {

	}

}
