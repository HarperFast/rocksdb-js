{
	'targets': [
		{
			'target_name': 'rocksdb-js',
			'dependencies': ['prepare-rocksdb'],
			'include_dirs': [
				'deps/rocksdb/include',
			],
			'sources': [
				'src/binding/binding.cpp',
				'src/binding/database.cpp',
				'src/binding/database_events.cpp',
				'src/binding/db_descriptor.cpp',
				'src/binding/db_handle.cpp',
				'src/binding/db_iterator.cpp',
				'src/binding/db_iterator_handle.cpp',
				'src/binding/db_registry.cpp',
				'src/binding/db_settings.cpp',
				'src/binding/transaction_handle.cpp',
				'src/binding/transaction.cpp',
				'src/binding/transaction_log.cpp',
				'src/binding/transaction_log_file.cpp',
				'src/binding/transaction_log_handle.cpp',
				'src/binding/transaction_log_store.cpp',
				'src/binding/util.cpp',
			],
			'defines': [
				'NAPI_VERSION=9',
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
							'<(module_root_dir)/deps/rocksdb/lib/librocksdb.a'
						]
					},
					'xcode_settings': {
						'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
					}
				}]
			],
			'configurations': {
				'Release': {
					# 'defines': ['DEBUG'],
					'msvs_settings': {
						'VCCLCompilerTool': {
							'RuntimeLibrary': 2,
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
							# 'RuntimeLibrary': 3,
							'RuntimeLibrary': 2,
							'ExceptionHandling': 1,
							'AdditionalOptions!': [],
							# 'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20']
							'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20', '/U_DEBUG']
						},
						'VCLinkerTool': {
							'AdditionalLibraryDirectories': [
								# '<(module_root_dir)/deps/rocksdb/debug/lib'
								'<(module_root_dir)/deps/rocksdb/lib'
							],
							'AdditionalDependencies': [
								# 'rocksdbd.lib'
								'rocksdb.lib'
							]
						}
					},
					'xcode_settings': {
						'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
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
