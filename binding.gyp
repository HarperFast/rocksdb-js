{
	'targets': [
		{
			'cflags!': [ '-fno-exceptions' ],
			'cflags_cc!': [ '-fno-exceptions' ],
			'cflags_cc': ['-std=c++20'],
			'dependencies': ['prepare-rocksdb'],
			'include_dirs': [
				'deps/rocksdb/include',
			],
			'link_settings': {
				'conditions': [
					['OS=="win"', {
						'libraries': [
							'<(module_root_dir)/deps/rocksdb/lib/rocksdb.lib'
						]
					}, {
						'libraries': [
							'<(module_root_dir)/deps/rocksdb/lib/librocksdb.a'
						]
					}]
				]
			},
			'target_name': 'rocksdb-js',
			'sources': [
				'src/binding/binding.cpp',
				'src/binding/database.cpp',
			],
			'conditions': [
				['OS=="mac"', {
					'cflags+': ['-fvisibility=hidden'],
					'xcode_settings': {
						'GCC_SYMBOLS_PRIVATE_EXTERN': 'YES', # -fvisibility=hidden
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