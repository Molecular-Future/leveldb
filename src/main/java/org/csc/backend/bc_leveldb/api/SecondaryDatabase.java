package org.csc.backend.bc_leveldb.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.csc.backend.bc_leveldb.jni.LDBNative;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Slf4j
public class SecondaryDatabase extends LDatabase {

	String secondaryKey;

	public SecondaryDatabase(long dbinst, LDBNative nativeInst, String dbfilename) {
		super(dbinst, nativeInst, dbfilename);
	}

	public static byte[] appendKeys(byte existdb[], byte[] newvalues) {
		if (existdb != null) {
			for (byte[] exkey : parseArray(existdb)) {
				if (Arrays.equals(newvalues, exkey)) {
					return null;
				}
			}
		}
		byte serialobj[];
		int offset = 0;
		if (existdb != null) {
			serialobj = new byte[newvalues.length + 2 + existdb.length];
			System.arraycopy(existdb, 0, serialobj, 0, existdb.length);
			offset = existdb.length;
		} else {
			serialobj = new byte[newvalues.length + 2];
		}
		serialobj[offset] = (byte) (newvalues.length & 0xff);// key==256?
		serialobj[offset + 1] = (byte) (newvalues.length >> 8 & 0xff);// key==256?
		System.arraycopy(newvalues, 0, serialobj, offset + 2, newvalues.length);
		return serialobj;
	}

	public static List<byte[]> parseArray(byte existdb[]) {
		ArrayList<byte[]> keys = new ArrayList<>();
		int offset = 0;
		while (offset + 2 < existdb.length) {
			int len = (existdb[offset] & 0xff) | ((existdb[offset + 1] & 0xff) << 8);
			byte[] key = new byte[len];
			System.arraycopy(existdb, offset + 2, key, 0, len);
			keys.add(key);
			offset += 2 + len;
		}
		return keys;
	}

	public List<byte[]> listBySecondary(byte[] secondkeys) {
		byte[] existdb = fastGet(secondkeys);
		if (existdb != null) {
			return parseArray(existdb);
		} else {
			return null;
		}
	}

	public static int removeKey(byte existdb[], byte[] key, byte replaceKeys[]) {
		// byte replaceKeys[] = new byte[existdb.length];
		int offset = 0;
		int dstoffset = 0;
		while (offset + 2 < existdb.length) {
			int len = (existdb[offset] & 0xff) | ((existdb[offset + 1] & 0xff) << 8);
			offset = offset + 2;
			int i = 0;
			for (i = 0; i < len; i++) {
				if (i >= key.length || key[i] != existdb[offset + i]) {
					break;
				}
			}
			if (i < len) {
				//
				System.arraycopy(existdb, offset - 2, replaceKeys, dstoffset, len + 2);
				dstoffset = dstoffset + len + 2;
			}
			offset += len;
		}
		return dstoffset;
	}

	public synchronized void putBySecondaryKeys(byte[] key, byte[] value, byte[] secondKey) {
		byte[] existdb = fastGet(secondKey);
		byte newkeys[] = appendKeys(existdb, key);
		if (newkeys != null) {
			super.syncPut(secondKey, newkeys);
		}
	}
	
	public void delete(byte[] secondaryKey, byte[] delKey) {
		byte existdb[] = super.fastGet(secondaryKey);
		if (existdb != null) {
			byte newret[] = new byte[existdb.length];
			int newlen = removeKey(existdb, delKey, newret);
			byte newkeys[] = new byte[newlen];
			System.arraycopy(newret, 0, newkeys, 0, newlen);
			super.syncPut(secondaryKey, newkeys);
		}
	}
}
