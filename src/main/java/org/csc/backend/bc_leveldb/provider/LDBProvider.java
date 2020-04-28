package org.mos.backend.bc_leveldb.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.ServiceProperty;
import org.apache.felix.ipojo.annotations.Validate;
import org.mos.core.dbapi.ODBSupport;
import org.mos.backend.bc_leveldb.jni.LDBNative;
import org.fc.zippo.dispatcher.IActorDispatcher;
import org.osgi.framework.BundleContext;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import onight.tfw.mservice.NodeHelper;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.StoreServiceProvider;
import onight.tfw.outils.conf.PropHelper;

@Component(publicFactory = false)
@Instantiate(name = "bc_db")
@Provides(specifications = { StoreServiceProvider.class, ActorService.class }, strategy = "SINGLETON")
@Slf4j
@Data
public class LDBProvider implements StoreServiceProvider, ActorService {

	@ServiceProperty(name = "name")
	String name = "bc_db";

	BundleContext bundleContext;
	@Setter
	@Getter
	String rootPath = "db";
	private HashMap<String, ODBSupport> dbsByDomains = new HashMap<>();
	// private Environment dbEnv = null;
	@ActorRequire(name = "zippo.ddc", scope = "global")
	IActorDispatcher dispatcher = null;

	public IActorDispatcher getDispatcher() {
		return dispatcher;
	}

	public void setDispatcher(IActorDispatcher dispatcher) {
		log.info("setDispatcher==" + dispatcher);
		this.dispatcher = dispatcher;
	}

	public LDBProvider(BundleContext bundleContext) {
		this.bundleContext = bundleContext;
		params = new PropHelper(bundleContext);
		// log.debug("new LDBProvider:" + this.hashCode() + ":" + this);
	}

	@Override
	public String[] getContextConfigs() {
		return new String[] {};
	}

	PropHelper params;

	OLevelDBImpl default_dbImpl;

	LDBHelper dbHelper;

	@Validate
	public void startup() {
		try {
			log.debug("InitDBStart:::" + this);

			new Thread(new DBStartThread()).start();
		} catch (Throwable t) {
			log.error("init bc bdb failed", t);
		}
	}

	public static LDBNative nativeLDB = null;

	class DBStartThread extends Thread {
		@Override
		public void run() {
			try {
				while (dispatcher == null) {
					try {
						log.debug("wait for dispatcher.");
						Thread.sleep(1000);
					} catch (Exception e) {
					}
				}
				String loadpath = params.get("org.mos.leveldb.clib.path", "./clib/leveldb");

				try {
					LDBNative.loadLibrary(loadpath);
					nativeLDB = new LDBNative();
					nativeLDB.init();
					log.debug("leveldb clib init success.");
				} catch (Throwable e) {
					log.error("unable to load level db in path:" + loadpath, e);
					System.exit(-1);
				}
				dbHelper = new LDBHelper(params, dispatcher, nativeLDB);
				String dir = params.get("org.mos.ldb.dir",
						"ldb." + Math.abs(NodeHelper.getCurrNodeListenOutPort() - 5100));

				synchronized (dbsByDomains) {
					for (String domainName : dbsByDomains.keySet()) {
						dbHelper.createDBI(dbsByDomains, dir, domainName);
					}
				}
			} catch (Exception e) {
				log.error("db start error::", e);
			}
		}
	}

	@Invalidate
	public void shutdown() {
		Iterator<String> it = this.dbsByDomains.keySet().iterator();
		while (it.hasNext()) {
			try {
				ODBSupport odb = this.dbsByDomains.get(it.next());
				if (odb != null && odb instanceof OLevelDBImpl) {
					((OLevelDBImpl) odb).close();
				} else if (odb != null && odb instanceof SlicerDBImpl) {
					((SlicerDBImpl) odb).close();
				} else if (odb != null && odb instanceof TimeShardDBImpl) {
					((TimeShardDBImpl) odb).close();
				}

			} catch (RuntimeException e) {
				log.warn("close db error", e);
			}
		}
	}

	@Override
	public String getProviderid() {
		return "bc_db";
	}

	private List<String> tempDomainName = new ArrayList<String>();

	@Override
	public DomainDaoSupport getDaoByBeanName(DomainDaoSupport dds) {
		ODBSupport dbi = dbsByDomains.get(dds.getDomainName());
		if (dbi == null) {
			while (dbHelper == null) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
				}
			}
			String dir = params.get("org.mos.ldb.dir", "ldb." + Math.abs(NodeHelper.getCurrNodeListenOutPort() - 5100));
			if (log.isDebugEnabled()) {
				log.debug("===>DB.path: {}", dir);
			}
			dbi = dbHelper.createDBI(dbsByDomains, dir, dds.getDomainName());
		}
		return dbi;
	}

}
