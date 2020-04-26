package org.csc.backend.bc_leveldb.api;

public enum StatusCode {

	kOk, // = 0,
	kNotFound, // = 1,
	kCorruption, // = 2,
	kNotSupported, // = 3,
	kInvalidArgument, // = 4,
	kIOError,// = 5
}
