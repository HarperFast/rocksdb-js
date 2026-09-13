{
	'variables': {
		# Run the init-gtest script at gyp parse time so the gtest source tree
		# exists on disk before make generates pattern rules for it. GNU make
		# resolves implicit pattern rules at startup, so the source .cc files
		# must already exist when the makefile is loaded - an order-only dep
		# on a download action is too late.
		# The .ts script runs directly under Node's native type stripping (no tsx).
		# The path is quoted so a repo checkout under a path with spaces still runs:
		# GYP passes this through a shell, which would otherwise split an unquoted
		# `<(module_root_dir)` at the space (`/bin/sh: /path/to/rocksdb: not found`).
		'_init_gtest': '<!(node "<(module_root_dir)/scripts/init-gtest/main.ts")',
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
						# Also set per configuration: a configuration's msvs_settings is
						# merged last, so it would otherwise win over the target's.
						'ExceptionHandling': 1,
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
			'Debug': {
				'msvs_settings': {
					'VCCLCompilerTool': {
						'ExceptionHandling': 1,
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
			# gtest's own translation units need exceptions, not just the targets
			# that include its headers: node's common.gypi disables them by default,
			# and GTEST_HAS_EXCEPTIONS is derived per TU, so gtest-all.cc would
			# compile the shared headers with 0 while binding.gyp's test targets
			# compile them with 1. See AGENTS.md ("Test Structure") for what that
			# mismatch costs.
			'cflags!': ['-fno-exceptions'],
			'cflags_cc!': ['-fno-exceptions'],
			'cflags_cc': ['-std=c++20', '-fexceptions'],
			'xcode_settings': {
				'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
			},
			'msvs_settings': {
				'VCCLCompilerTool': {
					'ExceptionHandling': 1,
					'AdditionalOptions': ['/Zc:__cplusplus', '/std:c++20']
				}
			}
		},
	]
}
