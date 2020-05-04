package org.mos.backend.bc_leveldb.provider;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.mos.core.dbapi.ODBException;
import org.mos.core.dbapi.ODBSupport;
import org.mos.tools.bytes.BytesComparisons;
import org.mos.tools.bytes.BytesHashMap;
import org.mos.backend.bc_leveldb.api.LDatabase;
import org.mos.backend.bc_leveldb.api.SecondaryDatabase;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.tfw.mservice.ThreadContext;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.ServiceSpec;

@Slf4j
@Data
public class OLevelDBImpl implements ODBSupport, DomainDaoSupport {
	String domainName = "";
	String subSliceName = "";
	private LDatabase dbs;
	private SecondaryDatabase sdb = null;

	private boolean autoSync = true;
	AtomicInteger relayWriteCounter = new AtomicInteger(0);

	public OLevelDBImpl(String domain, String subSliceName, LDatabase dbs) {
		this.domainName = domain;
		this.subSliceName = subSliceName;
		this.dbs = dbs;
	}

	public OLevelDBImpl(String domain, String subSliceName, LDatabase dbs, LDatabase sdbs) {
		this.domainName = domain;
		this.dbs = dbs;
		this.subSliceName = subSliceName;
		this.sdb = (SecondaryDatabase) sdbs;
	}

	@Override
	public DomainDaoSupport getDaosupport() {
		if (dbs != null) {
			return this;
		} else {
			return null;
		}
	}

	public OLevelDBImpl ensureOpen() {
		dbs.ensureOpen();
		if (sdb != null) {
			sdb.ensureOpen();
		}
		return this;
	}

	@Override
	public Class<?> getDomainClazz() {
		return Object.class;
	}

	@Override
	public String getDomainName() {
		return "leveldb";
	}

	public void close() {
		if (dbs != null) {
			dbs.close();
		}
		if (sdb != null) {
			sdb.close();
		}
	}

	public void sync() {
		dbs.sync();
		sdb.sync();
	}

	@Override
	public ServiceSpec getServiceSpec() {
		return new ServiceSpec("oleveldb");
	}

	@Override
	public void setDaosupport(DomainDaoSupport dao) {
		log.trace("setDaosupport::dao=" + dao);
	}

	@Override
	public Future<byte[][]> batchDelete(List<byte[]> keys) throws ODBException {
		for (byte[] key : keys) {
			dbs.syncDelete(key);
		}
		return ConcurrentUtils.constantFuture(keys.toArray(new byte[][] {}));
	}

	@Override
	public Future<byte[][]> batchPuts(List<byte[]> keys, List<byte[]> values) throws ODBException {
		try {
			dbs.syncBatchPut(keys.toArray(new byte[][] {}), values.toArray(new byte[][] {}));
		} catch (Exception e) {
			throw new ODBException("list error:", e);
		}
		return ConcurrentUtils.constantFuture(null);
	}

	@Override
	public Future<byte[]> delete(byte[] key) throws ODBException {
		// if (sdb != null) {
		// sdb.delete(key, dbs.fastGet(key.toByteArray()));
		// }
		int putret = dbs.syncDelete(key);

		if (putret == 0) {
			return ConcurrentUtils.constantFuture(null);
		} else {
			throw new ODBException("delete error:" + putret);
		}
	}

	// TODO: 未实现事务
	@Override
	public Future<BytesHashMap<byte[]>> deleteBySecondKey(byte[] secondaryKey, List<byte[]> keys) throws ODBException {
		try {
			List<byte[]> list = sdb.listBySecondary(secondaryKey);
			if (list != null) {
				for (byte[] sKey : list) {
					for (byte[] oKey : keys) {
						if (BytesComparisons.equal(oKey, sKey)) {
							delete(oKey);
							sdb.delete(secondaryKey, oKey);
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("error on deleteBySecondKey", e);
		}

		return ConcurrentUtils.constantFuture(null);
	}

	@Override
	public Future<byte[]> get(byte[] key) throws ODBException {
		byte bb[] = null;

		if (ThreadContext.getContext("__LDB_FILLGET") != null) {
			bb = dbs.fillGet(key);
		} else {
			bb = dbs.fastGet(key);
		}
		if (bb == null) {
			return ConcurrentUtils.constantFuture(null);
		} else {
			return ConcurrentUtils.constantFuture(bb);
		}
	}

	@Override
	public Future<byte[][]> list(List<byte[]> keys) throws ODBException {
		List<byte[]> list = new ArrayList<>();
		try {
			for (byte[] key : keys) {
				list.add(get(key).get());
			}
		} catch (Exception e) {
			throw new ODBException("list error:", e);
		}
		return ConcurrentUtils.constantFuture(list.toArray(new byte[][] {}));
	}

	@Override
	public Future<BytesHashMap<byte[]>> listBySecondKey(byte[] secondaryKey) throws ODBException {
		BytesHashMap<byte[]> ret = new BytesHashMap<>();
		if (sdb != null) {
			List<byte[]> keys = sdb.listBySecondary(secondaryKey);
			if (keys != null) {
				for (byte[] k : keys) {
					byte[] v = dbs.fastGet(k);
					if (v != null) {
						ret.put(k, v);
					}
				}
			}

		}
		return ConcurrentUtils.constantFuture(ret);
	}

	@Override
	public Future<byte[]> put(byte[] key, byte[] v) throws ODBException {
		int putret = dbs.syncPut(key, v);
		if (putret == 0) {
			return ConcurrentUtils.constantFuture(v);
		} else {
			return ConcurrentUtils.constantFuture(null);
		}
	}

	@Override
	public Future<byte[]> put(byte[] key, byte[] v, byte[] secondaryKey) throws ODBException {
		int putret = dbs.syncPut(key, v);
		if (putret == 0) {
			if (sdb != null) {
				sdb.putBySecondaryKeys(key, v, secondaryKey);
			}
			return ConcurrentUtils.constantFuture(v);
		} else {
			return ConcurrentUtils.constantFuture(null);
		}
	}

	@Override
	public Future<byte[]> putIfNotExist(byte[] key, byte[] v) throws ODBException {
		byte bb[] = dbs.fastGet(key);
		if (bb == null) {
			return put(key, v);
		} else {
			return ConcurrentUtils.constantFuture(null);
		}
	}

	@Override
	public void deleteAll() throws ODBException {
		dbs.deleteAll();
	}

}
