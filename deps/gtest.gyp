{
	'targets': [
		{
			'target_name': 'prepare-gtest',
			'type': 'none',
			'actions': [
				{
					'action_name': 'prepare_gtest',
					'message': 'Preparing GoogleTest...',
					'action': [
						'<(module_root_dir)/node_modules/.bin/tsx',
						'<(module_root_dir)/scripts/init-gtest/main.ts',
					],
					'inputs': [
						'<(module_root_dir)/scripts/init-gtest/main.ts',
					],
					'outputs': [
						'googletest/googletest/include/gtest/gtest.h',
					],
				}
			]
		},
		{
			'target_name': 'gtest',
			'type': 'static_library',
			'dependencies': ['prepare-gtest'],
			'include_dirs': [
				'googletest/googletest/include',
				'googletest/googletest',
				'googletest/googlemock/include',
			],
			'sources': [
				'googletest/googletest/src/gtest-all.cc',
			],
			'direct_dependent_settings': {
				'include_dirs': [
					'googletest/googletest/include',
					'googletest/googlemock/include',
				],
			},
			'cflags_cc': ['-std=c++20'],
			'msvs_settings': {
				'VCCLCompilerTool': {
					'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20']
				}
			}
		},
		{
			'target_name': 'gtest_main',
			'type': 'static_library',
			'dependencies': ['gtest'],
			'sources': [
				'googletest/googletest/src/gtest_main.cc',
			],
			'cflags_cc': ['-std=c++20'],
			'msvs_settings': {
				'VCCLCompilerTool': {
					'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20']
				}
			}
		},
	]
}
