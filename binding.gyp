{
	'targets': [
		{
			'defines': ['NODE_ADDON_API_DISABLE_CPP_EXCEPTIONS'],
			'dependencies': ['prepare-rocksdb'],
			'include_dirs': [
				'<!(node -p "require(\'node-addon-api\').include_dir")',
				'deps/rocksdb/include'
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
				'src/binding/rocksdb-js.cpp'
			],
			'configurations': {
				'Release': {
					'msvs_settings': {
						'VCCLCompilerTool': {
							'RuntimeLibrary': 2,
							'ExceptionHandling': 1
						}
					}
				},
				'Debug': {
					'msvs_settings': {
						'VCCLCompilerTool': {
							'RuntimeLibrary': 3,
							'ExceptionHandling': 1
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