package org.csc.backend.bc_leveldb.jni;

import org.csc.backend.bc_leveldb.api.TransactionConfig;

import java.lang.reflect.Field;

public class LDBNative {
//
//	@AllArgsConstructor
//	@NoArgsConstructor
//	@Data
//	public static class Option {
//		int delta;
//	}

	public native void init();

	public native long openDB(TransactionConfig.Option option, String dbname);

	public native void closeDB(long ptr);

	// flatten
	public native int syncPut(long dbinst, byte[] key, byte[] value);

	// Remove the database entry (if any) for "key". Returns OK on
	// success, and a non-OK status on error. It is not an error if "key"
	// did not exist in the database.
	// Note: consider setting options.sync = true.
	public native int syncDelete(long dbinst, byte[] key);

	// flatten
	// Apply the specified updates to the database.
	// Returns OK on success, non-OK on failure.
	// Note: consider setting options.sync = true.
	public native int syncBatchPut(long dbinst, byte[][] keys, byte[][] values);

	// If the database contains an entry for "key" store the
	// corresponding value in *value and return OK.
	//
	// If there is no entry for "key" leave *value unchanged and return
	// a status for which StatusCode::IsNotFound() returns true.

	// May return some other StatusCode on an error.
	// verify_checksums=false,fill_cache=false,snapshot=NULL
	public native byte[] fastGet(long dbinst, byte[] key);

	// verify_checksums=false,fill_cache=true,snapshot=NULL
	public native byte[] fillGet(long dbinst, byte[] key);

	public static LDBNative loadLibrary() throws Throwable {
		return loadLibrary(null);
	}

	/**
	 * Sets the java library path to the specified path
	 *
	 * @param path
	 *            the new library path
	 * @throws Exception
	 */
	public static void setLibraryPath(String path) throws Exception {
		System.setProperty("java.library.path", path);
		// set sys_paths to null
		final Field sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
		sysPathsField.setAccessible(true);
		sysPathsField.set(null, null);
	}

	private static String OS = System.getProperty("os.name").toLowerCase();

	public static boolean isWindows() {
		return (OS.indexOf("win") >= 0);
	}

	public static LDBNative loadLibrary(String path) throws Throwable {
		if (path == null) {
			path = "./clib";
		}
		String libpath = System.getProperty("java.library.path");
		if (isWindows()) {
			libpath = libpath + ";" + path;
		} else {
			libpath = libpath + ":" + path;
		}
		setLibraryPath(libpath);
		System.out.println("libpath=" + libpath);
		System.loadLibrary("leveldb");
		return new LDBNative();
	}

}
