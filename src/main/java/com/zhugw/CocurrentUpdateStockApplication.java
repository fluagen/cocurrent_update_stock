package com.zhugw;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootApplication
public class CocurrentUpdateStockApplication implements CommandLineRunner {
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Bean
	JedisConnectionFactory jedisConnectionFactory() {
		return new JedisConnectionFactory();
	}

	@Bean
	RedisTemplate<String, Long> redisTemplate() {
		final RedisTemplate<String, Long> template = new RedisTemplate<String, Long>();
		template.setConnectionFactory(jedisConnectionFactory());
		template.setKeySerializer(new StringRedisSerializer());
		template.setHashValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		template.setValueSerializer(new GenericToStringSerializer<Long>(Long.class));
		return template;
	}

	public static void main(String[] args) {
		SpringApplication.run(CocurrentUpdateStockApplication.class, args);
	}

	private Callable<Void> updateStockInMysqlTask = () -> {
		final String sql = "update award_count set stock = stock-1 where award_id=1 and stock>0";
		jdbcTemplate.update(sql);
		return null;
	};
	private Callable<Void> updateStockInRedisTask = () -> {
		redisTemplate().execute(new RedisCallback<Long>() {
			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				Long incr = connection.decr("1_stock".getBytes());
				return incr;
			}
		});
		return null;
	};

	@Override
	public void run(String... args) throws Exception {
		final String name = "redis"; // or "redis"
		System.out.printf("start concurrent update stock in %s...%n", name);
		List<Long> timeList = new ArrayList<>();
		for (int i = 0; i < 10; i++) {//分别统计10次
			long start = System.currentTimeMillis();
			concurrentUpdateStock(name); //
			long end = System.currentTimeMillis();
			System.out.printf("Done. Take time: %d ms%n", end - start);
			timeList.add(end - start);
			Thread.sleep(1000);
		}
		System.out.println(timeList.stream().collect(Collectors.summarizingLong(t -> t))); //输出统计结果

	}

	private void concurrentUpdateStock(String name) throws InterruptedException {
		// 模拟并发更新库存
		int nThreads = 500; //设置一个较大线程数
		ExecutorService pool = Executors.newFixedThreadPool(nThreads);
		List<Callable<Void>> tasks = new ArrayList<>();
		for (int i = 0; i < nThreads * 2; i++) { //2倍于线程数的减库存任务
			if ("mysql".equalsIgnoreCase(name))
				tasks.add(updateStockInMysqlTask);
			else if ("redis".equalsIgnoreCase(name))
				tasks.add(updateStockInRedisTask);
		}
		List<Future<Void>> futureList = pool.invokeAll(tasks);

		while (futureList.stream().anyMatch(f -> !f.isDone())); //等待任务执行完
		pool.shutdown();

	}

}
