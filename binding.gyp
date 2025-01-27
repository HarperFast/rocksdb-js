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
				'vendor/rocksdb-<(rocksdb_version)/include'
			],
			'link_settings': {
				'libraries': [
					'<(module_root_dir)/vendor/rocksdb-<(rocksdb_version)/librocksdb.a'
				]
			},
			'target_name': 'rocksdb-js',
			'sources': [
				'src/binding/rocksdb-js.cpp'
			]
		},
		{
			'actions': [
				{
					'action_name': 'build_rocksdb',
					'inputs': [
						'vendor/rocksdb-<(rocksdb_version)/Makefile',
					],
					'outputs': [
						'vendor/rocksdb-<(rocksdb_version)/librocksdb.a',
					],
					'action': [
						'make',
						'-C', 'vendor/rocksdb-<(rocksdb_version)',
						'static_lib',
					],
					'message': 'Building RocksDB...'
				}
			],
			'target_name': 'rocksdb',
			'type': 'none',
		}
	]
}