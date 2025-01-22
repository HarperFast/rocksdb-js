{
	'targets': [
		{
			'target_name': 'rocksdb',
			'include_dirs'  : [
				'<!(node -e "require(\'napi-macros\')")'
			],
			'sources': [
				'src/binding/rocksdb.cpp'
			]
		}
	]
}