package org.mos.backend.bc_leveldb.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.mos.core.dbapi.ODBException;
import org.mos.tools.bytes.BytesHashMap;
import org.mos.backend.bc_leveldb.api.LDatabase;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeferOBDBImpl extends OLevelDBImpl implements Runnable {

	public BytesHashMap<byte[]> memoryMap = new BytesHashMap<>();
	public BytesHashMap<byte[]> l2CacheMap = new BytesHashMap<>();
	int maxSize = 100;
	int maxL2CacheSize = 1000; // 1000
	long delayWriteMS = 200;
	AtomicInteger curMemorySize = new AtomicInteger(0);
	AtomicBoolean dbsyncing = new AtomicBoolean(false);

	public DeferOBDBImpl(int maxSize, int maxL2CacheSize, long delayWriteMS, String domain, String subSliceName,
			LDatabase dbs, LDatabase sdbs) {
		super(domain, subSliceName, dbs, sdbs);
		this.maxSize = maxSize;
		this.maxL2CacheSize = maxL2CacheSize;
		this.delayWriteMS = delayWriteMS;
	}

	public DeferOBDBImpl(int maxSize, int maxL2CacheSize, long delayWriteMS, String domain, String subSliceName,
			LDatabase dbs) {
		super(domain, subSliceName, dbs);
		this.maxSize = maxSize;
		this.maxL2CacheSize = maxL2CacheSize;
		this.delayWriteMS = delayWriteMS;
	}

	@Override
	public synchronized Future<byte[][]> batchPuts(List<byte[]> keys, List<byte[]> values) throws ODBException {
		// return super.batchPuts(keys, values);
		curMemorySize.addAndGet(keys.size());
		for (int i = 0; i < keys.size(); i++) {
			memoryMap.put(keys.get(i), values.get(i));
		}
		deferSync();
		return ConcurrentUtils.constantFuture(null);
	}

	long lastSyncTime = 0;
	long lastL2SyncTime = 0;

	public synchronized void deferSync() {
		Thread.currentThread().setName("db-defersync-" + domainName + subSliceName);
		if (curMemorySize.get() >= maxSize
				|| (System.currentTimeMillis() - lastSyncTime > delayWriteMS) && curMemorySize.get() > 0) {
			if (l2CacheMap.size() == 0 && dbsyncing.compareAndSet(false, true)) {
				// log.error("deferdb." + domainName + subSliceName + ".l2cache-sync-put.size="
				// + curMemorySize.get()
				// + ",memoryMapsize=" + memoryMap.size() + ",maxSize=" + maxSize + ",sync=" +
				// dbsyncing.get());
				l2CacheMap = memoryMap;
				memoryMap = new BytesHashMap<byte[]>();
				curMemorySize.set(0);
				lastSyncTime = System.currentTimeMillis();
				// new Thread(this).start();
			} else {
				// log.error("deferdb." + domainName + subSliceName +
				// ".l2cache-sync-waiting....size="
				// + curMemorySize.get() + ",memoryMapsize=" + memoryMap.size() + ",maxSize=" +
				// maxSize + ",sync="
				// + dbsyncing.get());

			}
		}
		Thread.currentThread().setName("dbpools");
	}

	public synchronized void dbCacheSync() {

		Thread.currentThread().setName("db-L2cachesync-" + domainName + subSliceName);
		try {
			if (l2CacheMap.size() == 0) {
				return;
			}
			int size = l2CacheMap.size();
			byte[][] keys = new byte[size][];
			byte[][] values = new byte[size][];
			// List<byte[]> values = new ArrayList<>();
			// log.error("deferdb." + domainName + subSliceName +
			// ".l2cache-sync-size.start=" + size);
			// long start = System.currentTimeMillis();
			int cc = 0;
			for(Map.Entry<byte[],byte[]> kp:l2CacheMap.entrySet()) {
				keys[cc] = kp.getKey();
				values[cc] = kp.getValue();
				cc = cc + 1;
			}

			super.getDbs().syncBatchPut(keys, values);
			// log.error("deferdb." + domainName + subSliceName + ".l2cache-sync-size.end="
			// + size + ",cc=" + cc + ",cost="
			// + (System.currentTimeMillis() - start));
			l2CacheMap.clear();
			getDbs().sync();
			lastL2SyncTime = System.currentTimeMillis();
		} catch (Throwable t) {
			log.error("derferdb." + domainName + subSliceName + ".l2dbCacheSync error:" + domainName + subSliceName, t);
		} finally {
			dbsyncing.compareAndSet(true, false);
			Thread.currentThread().setName("db-L2cachesync-" + domainName + subSliceName);
		}
	}

	@Override
	public void close() {
		deferSync();
		dbCacheSync();
		super.close();
	}

	@Override
	public synchronized Future<byte[]> put(byte[] key, byte[] v) throws ODBException {
		// return super.put(key, v);
		curMemorySize.incrementAndGet();
		memoryMap.put(key,v);

		deferSync();
		return ConcurrentUtils.constantFuture(v);
	}

	@Override
	public Future<byte[]> get(byte[] key) throws ODBException {
		byte[]ov = memoryMap.get(key);
		if (ov != null)
			return ConcurrentUtils.constantFuture(ov);
		ov = l2CacheMap.get(key);
		if (ov != null)
			return ConcurrentUtils.constantFuture(ov);

		return super.get(key);
	}

	@Override
	public synchronized Future<byte[]> delete(byte[] key) throws ODBException {
		// log.error("deferdb." + domainName + subSliceName + " delete.one");
		memoryMap.remove(key);
		// l2CacheMap.remove(key);
		return super.delete(key);
	}

	@Override
	public synchronized Future<byte[][]> batchDelete(List<byte[]> keys) throws ODBException {
		// log.error("deferdb." + domainName + subSliceName + " batchdelete.size=" +
		// keys.size());
		for (byte[] key : keys) {
			memoryMap.remove(key);
			// l2CacheMap.remove(key);
		}
		return super.batchDelete(keys);
	}

	public synchronized Future<byte[][]> list(List<byte[]> keys) throws ODBException {
		List<byte[]> list = new ArrayList<>();
		for (byte[] key : keys) {
			Future<byte[]> ov = get(key);
			try {
				if (ov != null && ov.get() != null) {
					list.add(ov.get());
				}
			} catch (Exception e) {

			}
		}
		return ConcurrentUtils.constantFuture(list.toArray(new byte[][] {}));

	}

	@Override
	public void run() {
		deferSync();
		dbCacheSync();

	}
}
