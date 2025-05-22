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
				'src/binding/db_descriptor.cpp',
				'src/binding/db_handle.cpp',
				'src/binding/db_iterator.cpp',
				'src/binding/db_registry.cpp',
				'src/binding/db_settings.cpp',
				'src/binding/transaction_handle.cpp',
				'src/binding/transaction.cpp',
				'src/binding/util.cpp',
			],
			'link_settings': {
				'conditions': [
					['OS=="win"', {
						'libraries': [
							'<(module_root_dir)/deps/rocksdb/lib/rocksdb.lib',
							'rpcrt4.lib',
							'shell32.lib',
							'shlwapi.lib'
						]
					}, {
						'libraries': [
							'<(module_root_dir)/deps/rocksdb/lib/librocksdb.a'
						]
					}]
				]
			},
			'cflags!': [ '-fno-exceptions' ],
			'cflags_cc!': [ '-fno-exceptions' ],
			'cflags_cc': [
				'-std=c++20',
				'-fexceptions'
			],
			'conditions': [
				['OS=="linux" or OS=="mac"', {
					'cflags+': ['-fexceptions'],
					'cflags_cc+': ['-fexceptions'],
					'xcode_settings': {
						'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
					}
				}],
				['OS=="win"', {
					'msvs_settings': {
						'VCCLCompilerTool': {
							'ExceptionHandling': 1
						}
					}
				}]
			],
			'configurations': {
				'Release': {
					'msvs_settings': {
						'VCCLCompilerTool': {
							'RuntimeLibrary': 2,
							'ExceptionHandling': 1,
							'AdditionalOptions': ['/std:c++20']
						}
					}
				},
				'Debug': {
					'cflags_cc+': ['-g', '--coverage'],
					'defines': ['DEBUG'],
					'ldflags': ['--coverage'],
					'xcode_settings': {
						'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
						'OTHER_CFLAGS': ['-g', '--coverage'],
						'OTHER_LDFLAGS': ['--coverage']
					},
					'msvs_settings': {
						'VCCLCompilerTool': {
							'RuntimeLibrary': 3,
							'ExceptionHandling': 1,
							'AdditionalOptions': ['/std:c++20']
						}
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