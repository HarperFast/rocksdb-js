{
	'variables': {
		# Run the init-gtest script at gyp parse time so the gtest source tree
		# exists on disk before make generates pattern rules for it. GNU make
		# resolves implicit pattern rules at startup, so the source .cc files
		# must already exist when the makefile is loaded - an order-only dep
		# on a download action is too late.
		'_init_gtest': '<!(<(module_root_dir)/node_modules/.bin/tsx <(module_root_dir)/scripts/init-gtest/main.ts)',
	},
	# See binding.gyp: Node 26's Windows headers inject Clang ThinLTO options
	# (-flto=thin / /opt:lldltojobs=N) into every Release target, which MSVC's
	# cl.exe and lib.exe reject. Strip them here too so the gtest_main library
	# builds cleanly. Inert on the Linux/macOS generators.
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
			'target_name': 'gtest_main',
			'type': 'static_library',
			'sources': [
				'googletest/googletest/src/gtest-all.cc',
				'googletest/googletest/src/gtest_main.cc',
			],
			'include_dirs': [
				'googletest/googletest/include',
				'googletest/googletest',
				'googletest/googlemock/include',
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
	]
}
