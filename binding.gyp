{
	'variables': {
		'rocksdb_version': '9.10.0',
	},
	'targets': [
		{
			'defines': ['NODE_ADDON_API_DISABLE_CPP_EXCEPTIONS'],
			'dependencies': ['rocksdb'],
			'include_dirs': [
				'<!(node -p "require(\'node-addon-api\').include_dir")',
				'<(module_root_dir)/build/rocksdb/include'
			],
			'link_settings': {
				'libraries': [
					'<(module_root_dir)/build/rocksdb/librocksdb.a'
				]
			},
			'target_name': 'rocksdb-js',
			'sources': [
				'src/binding/rocksdb-js.cpp'
			]
		},
		{
			'target_name': 'rocksdb',
			'type': 'none',
			'actions': [
				{
					'action_name': 'build_rocksdb',
					'inputs': [
						'<(module_root_dir)/vendor/rocksdb-<(rocksdb_version)/Makefile',
					],
					'outputs': [
						'<(module_root_dir)/vendor/rocksdb-<(rocksdb_version)/librocksdb.a',
					],
					'action': [
						'make',
						'-C', 'vendor/rocksdb-<(rocksdb_version)',
						'static_lib',
					],
					'message': 'Building RocksDB...'
				},
				{
					'action_name': 'copy_rocksdb',
					'inputs': [
						'<(module_root_dir)/vendor/rocksdb-<(rocksdb_version)/librocksdb.a',
						'<(module_root_dir)/vendor/rocksdb-<(rocksdb_version)/include',
					],
					'outputs': [
						'<(module_root_dir)/build/rocksdb/librocksdb.a',
						'<(module_root_dir)/build/rocksdb/include',
					],
					'action': [
						'cp',
						'-r',
						'vendor/rocksdb-<(rocksdb_version)/librocksdb.a',
						'vendor/rocksdb-<(rocksdb_version)/include',
						'<(module_root_dir)/build/rocksdb/',
					],
					'message': 'Copying RocksDB library...'
				}
			]
		}
	]
}