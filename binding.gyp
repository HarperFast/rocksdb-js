{
	# ASan toggle. Read from the ROCKSDB_ASAN env var at configure time (node -p
	# is cross-platform and always present under node-gyp). `-D`/`GYP_DEFINES`
	# do not reliably override a target-scoped default under node-gyp's make
	# generator, so an explicit env read is used instead. Enable with:
	#   ROCKSDB_ASAN=1 node-gyp rebuild
	'variables': { "rocksdb_asan%": "<!(node -p \"process.env.ROCKSDB_ASAN==='1'?1:0\")" },
	# Node 26's Windows headers (common.gypi) inject Clang ThinLTO options
	# (-flto=thin and /opt:lldltojobs=N) into every Release target. The official
	# Node Windows build is now compiled with ClangCL + ThinLTO, but this addon
	# builds with MSVC (cl.exe / link.exe), which rejects those LLVM-only flags:
	# link.exe aborts with "LNK1117: syntax error in option 'opt:lldltojobs=2'".
	# Strip them from the MSVC tool invocations via regex exclusion (value- and
	# thin/full-agnostic). This lives under msvs_settings, so it is inert on the
	# Linux/macOS generators and leaves their LTO behavior untouched.
	'target_defaults': {
		'configurations': {
			'Release': {
				'msvs_settings': {
					'VCCLCompilerTool': {
						'AdditionalOptions/': [['exclude', 'flto'], ['exclude', 'lldltojobs']],
					},
					'VCLibrarianTool': {
						'AdditionalOptions/': [['exclude', 'flto'], ['exclude', 'lldltojobs']],
					},
					'VCLinkerTool': {
						'AdditionalOptions/': [['exclude', 'flto'], ['exclude', 'lldltojobs']],
					},
				},
			},
		},
	},
	'targets': [
		{
			'target_name': 'rocksdb-js',
			'dependencies': ['prepare-rocksdb'],
			'include_dirs': [
				'deps/rocksdb/include',
				'src/binding',
			],
			'sources': [
				'src/binding/binding.cpp',
				'src/binding/core/debug.cpp',
				'src/binding/core/platform.cpp',
				'src/binding/core/file_lock.cpp',
				'src/binding/core/verification_table.cpp',
				'src/binding/napi/event_emitter.cpp',
				'src/binding/napi/global_events.cpp',
				'src/binding/napi/helpers.cpp',
				'src/binding/database/backup.cpp',
				'src/binding/database/backup_stream.cpp',
				'src/binding/database/backup_transaction_logs.cpp',
				'src/binding/database/checkpoint.cpp',
				'src/binding/database/database.cpp',
				'src/binding/database/database_events.cpp',
				'src/binding/database/db_descriptor.cpp',
				'src/binding/database/db_handle.cpp',
				'src/binding/database/db_registry.cpp',
				'src/binding/database/db_settings.cpp',
				'src/binding/iterator/db_iterator.cpp',
				'src/binding/iterator/db_iterator_handle.cpp',
				'src/binding/transaction/transaction_handle.cpp',
				'src/binding/transaction/transaction.cpp',
				'src/binding/transaction_log/transaction_log.cpp',
				'src/binding/transaction_log/transaction_log_file.cpp',
				'src/binding/transaction_log/transaction_log_handle.cpp',
				'src/binding/transaction_log/transaction_log_recovery.cpp',
				'src/binding/transaction_log/transaction_log_store.cpp',
				'src/binding/transaction_log/transaction_log_store_registry.cpp',
			],
			'defines': [
				# Note: node-gyp defaults to NAPI_VERSION=8 (v12.22.0+,
				# v14.17.0+, v15.12.0+, 16.0.0 and all later versions)
				#
				# We can force NAPI_VERSION=9 (v18.17.0+, 20.3.0+, 21.0.0 and
				# all later versions) by uncommenting the line below:

				# 'NAPI_VERSION=9',
			],
			'cflags!': [ '-fno-exceptions', '-std=c++17' ],
			'cflags_cc!': [ '-fno-exceptions', '-std=c++17' ],
			'cflags_cc': [
				'-std=c++20',
				'-fexceptions'
			],
			'conditions': [
				['OS=="win"', {
					'link_settings': {
						'libraries': [
							'rpcrt4.lib',
							'shell32.lib',
							'shlwapi.lib'
						]
					},
					'msvs_settings': {
						'VCCLCompilerTool': {
							'ExceptionHandling': 1,
							'AdditionalOptions!': ['/Zc:__cplusplus', '-std:c++17'],
							'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20']
						},
						'VCLinkerTool': {
							'LinkTimeCodeGeneration': 1,
							'LinkIncremental': 1
						}
					}
				}],
				['OS=="linux" or OS=="mac"', {
					'cflags+': ['-fexceptions'],
					'cflags_cc+': ['-fexceptions'],
					'link_settings': {
						'libraries': [
							'<(module_root_dir)/deps/rocksdb/lib/librocksdb.a',
							# librocksdb.a references zlib (BuiltinZlibCompressor) but does not
							# bundle it; link the zlib static lib shipped alongside it in the
							# RocksDB prebuild so the compressor object resolves when the linker
							# pulls it in. Must come after librocksdb.a (GNU ld is order-sensitive).
							'<(module_root_dir)/deps/rocksdb/lib/libz.a'
						]
					},
					'xcode_settings': {
						'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
						'MACOSX_DEPLOYMENT_TARGET': '26.0',
						'CLANG_CXX_LANGUAGE_STANDARD': 'c++20',
					}
				}],
				# AddressSanitizer instrumentation for diagnosing heap corruption
				# in the binding. Enable with `ROCKSDB_ASAN=1 node-gyp rebuild`.
				# RocksDB is linked as a non-instrumented prebuilt static lib;
				# ASan's global malloc/free interceptors still catch overflows and
				# use-after-free in binding-owned allocations. The .node must be
				# run with the ASan runtime preloaded (Linux: LD_PRELOAD the
				# libasan shared lib; macOS Node deadlocks under injected ASan, so
				# use the standalone native-test target there instead).
				['rocksdb_asan==1 and (OS=="mac" or OS=="linux")', {
					'cflags+': ['-fsanitize=address', '-fno-omit-frame-pointer', '-g', '-O1'],
					'cflags_cc+': ['-fsanitize=address', '-fno-omit-frame-pointer', '-g', '-O1'],
					'ldflags+': ['-fsanitize=address'],
					'xcode_settings': {
						'OTHER_CFLAGS+': ['-fsanitize=address', '-fno-omit-frame-pointer', '-g', '-O1'],
						'OTHER_CPLUSPLUSFLAGS+': ['-fsanitize=address', '-fno-omit-frame-pointer', '-g', '-O1'],
						'OTHER_LDFLAGS+': ['-fsanitize=address'],
					}
				}]
			],
			'configurations': {
				'Release': {
					# 'defines': ['DEBUG'],
					'msvs_settings': {
						'VCCLCompilerTool': {
							'RuntimeLibrary': 0,
							'ExceptionHandling': 1,
							'AdditionalOptions!': [],
							'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20']
						},
						'VCLinkerTool': {
							'AdditionalLibraryDirectories': [
								'<(module_root_dir)/deps/rocksdb/lib'
							],
							'AdditionalDependencies': [
								'rocksdb.lib'
							]
						}
					}
				},
				'Debug': {
					'cflags_cc+': ['-g', '--coverage'],
					'defines': ['DEBUG'],
					'ldflags': ['--coverage'],
					'msvs_settings': {
						'VCCLCompilerTool': {
							# 'RuntimeLibrary': 1,
							'RuntimeLibrary': 0,
							'ExceptionHandling': 1,
							'AdditionalOptions!': [],
							# 'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20']
							'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20', '/U_DEBUG']
						},
						'VCLinkerTool': {
							'AdditionalLibraryDirectories': [
								# '<(module_root_dir)/deps/rocksdb/debug/lib',
								'<(module_root_dir)/deps/rocksdb/lib'
							],
							'AdditionalDependencies': [
								# 'rocksdbd.lib',
								'rocksdb.lib'
							]
						}
					},
					'xcode_settings': {
						'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
						'CLANG_CXX_LANGUAGE_STANDARD': 'c++20',
						'OTHER_CFLAGS': ['-g', '--coverage'],
						'OTHER_LDFLAGS': ['--coverage']
					}
				}
			}
		},
		{
			'target_name': 'rocksdb-js-native-tests',
			'type': 'executable',
			'dependencies': ['prepare-rocksdb', 'deps/gtest.gyp:gtest_main'],
			'include_dirs': [
				'deps/rocksdb/include',
				'src/binding',
				'deps/googletest/googletest/include',
				'deps/googletest/googlemock/include',
			],
			'sources': [
				'src/binding/core/debug.cpp',
				'src/binding/core/platform.cpp',
				'src/binding/core/file_lock.cpp',
				'src/binding/core/verification_table.cpp',
				'src/binding/transaction_log/transaction_log_file.cpp',
				'src/binding/transaction_log/transaction_log_recovery.cpp',
				'test/native/event_emitter_stub.cc',
				'test/native/rocksdb_version_test.cc',
				'test/native/encoding_test.cc',
				'test/native/file_lock_test.cc',
				'test/native/json_test.cc',
				'test/native/transaction_log_madvise_test.cc',
				'test/native/transaction_log_mmap_test.cc',
				'test/native/transaction_log_recovery_test.cc',
				'test/native/transaction_log_writev_test.cc',
				'test/native/verification_table_test.cc',
			],
			'defines': [
				'ROCKSDB_JS_NATIVE_TESTS',
				'ROCKSDB_JS_WRITEV=rocksdb_js_mock_writev',
				'ROCKSDB_JS_MADVISE=rocksdb_js_mock_madvise',
			],
			'cflags!': [ '-fno-exceptions', '-std=c++17' ],
			'cflags_cc!': [ '-fno-exceptions', '-std=c++17' ],
			'cflags_cc': [
				'-std=c++20',
				'-fexceptions'
			],
			'conditions': [
				['OS=="win"', {
					'link_settings': {
						'libraries': [
							'rpcrt4.lib',
							'shell32.lib',
							'shlwapi.lib'
						]
					},
					'msvs_settings': {
						'VCCLCompilerTool': {
							'ExceptionHandling': 1,
							'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20']
						},
						'VCLinkerTool': {
							'AdditionalLibraryDirectories': [
								'<(module_root_dir)/deps/rocksdb/lib'
							],
							'AdditionalDependencies': [
								'rocksdb.lib'
							]
						}
					}
				}],
				['OS=="linux" or OS=="mac"', {
					'cflags+': ['-fexceptions'],
					'cflags_cc+': ['-fexceptions'],
					'link_settings': {
						'libraries': [
							'<(module_root_dir)/deps/rocksdb/lib/librocksdb.a',
							# librocksdb.a references zlib (BuiltinZlibCompressor) but does not
							# bundle it; link the zlib static lib shipped alongside it in the
							# RocksDB prebuild so the compressor object resolves when the linker
							# pulls it in. Must come after librocksdb.a (GNU ld is order-sensitive).
							'<(module_root_dir)/deps/rocksdb/lib/libz.a'
						]
					},
					'xcode_settings': {
						'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
						'MACOSX_DEPLOYMENT_TARGET': '26.0',
						'CLANG_CXX_LANGUAGE_STANDARD': 'c++20',
					}
				}],
				# ASan for the standalone (no-Node) GoogleTest binary. Unlike the
				# .node, this executable runs natively under ASan on macOS, so it
				# is the local vehicle for catching corruption in the txn-log /
				# recovery / encoding code paths. Enable with ROCKSDB_ASAN=1.
				['rocksdb_asan==1 and (OS=="mac" or OS=="linux")', {
					'cflags+': ['-fsanitize=address', '-fno-omit-frame-pointer', '-g', '-O1'],
					'cflags_cc+': ['-fsanitize=address', '-fno-omit-frame-pointer', '-g', '-O1'],
					'ldflags+': ['-fsanitize=address'],
					'xcode_settings': {
						'OTHER_CFLAGS+': ['-fsanitize=address', '-fno-omit-frame-pointer', '-g', '-O1'],
						'OTHER_CPLUSPLUSFLAGS+': ['-fsanitize=address', '-fno-omit-frame-pointer', '-g', '-O1'],
						'OTHER_LDFLAGS+': ['-fsanitize=address'],
					}
				}]
			],
			'configurations': {
				'Release': {
					'msvs_settings': {
						'VCCLCompilerTool': {
							'RuntimeLibrary': 0,
							'ExceptionHandling': 1,
							'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20']
						},
						'VCLinkerTool': {
							'AdditionalLibraryDirectories': [
								'<(module_root_dir)/deps/rocksdb/lib'
							],
							'AdditionalDependencies': [
								'rocksdb.lib'
							]
						}
					}
				},
				'Debug': {
					'cflags_cc+': ['-g', '--coverage'],
					'ldflags': ['--coverage'],
					'msvs_settings': {
						'VCCLCompilerTool': {
							'RuntimeLibrary': 0,
							'ExceptionHandling': 1,
							'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20', '/U_DEBUG']
						},
						'VCLinkerTool': {
							'AdditionalLibraryDirectories': [
								'<(module_root_dir)/deps/rocksdb/lib'
							],
							'AdditionalDependencies': [
								'rocksdb.lib'
							]
						}
					},
					'xcode_settings': {
						'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
						'CLANG_CXX_LANGUAGE_STANDARD': 'c++20',
						'OTHER_CFLAGS': ['-g', '--coverage'],
						'OTHER_LDFLAGS': ['--coverage']
					}
				}
			}
		},
		{
			'target_name': 'prepare-rocksdb',
			'type': 'none',
			'actions': [
				{
					'action_name': 'prepare_rocksdb',
					'message': 'Preparing RocksDB...',
					'action': [
						'<(module_root_dir)/node_modules/.bin/tsx',
						'<(module_root_dir)/scripts/init-rocksdb/main.ts',
					],
					'inputs': [
						'<(module_root_dir)/scripts/init-rocksdb/main.ts',
					],
					'outputs': [
						'deps/rocksdb/include',
					],
				}
			]
		},
	]
}
