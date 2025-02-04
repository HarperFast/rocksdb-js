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
				'libraries': [
					'<(module_root_dir)/deps/rocksdb/librocksdb.a'
				]
			},
			'target_name': 'rocksdb-js',
			'sources': [
				'src/binding/rocksdb-js.cpp'
			]
		},
		{
			'target_name': 'prepare-rocksdb',
			'type': 'none',
			'actions': [
				{
					'action_name': 'prepare_rocksdb',
					'message': 'Preparing RocksDB...',
					'action': [
						'node_modules/.bin/tsx',
						'scripts/prepare-rocksdb.ts',
					],
					'inputs': [
						'scripts/prepare-rocksdb.ts',
					],
					'outputs': [
						'deps/rocksdb/include',
						'deps/rocksdb/librocksdb.a',
					],
				}
			]
		},
	]
}