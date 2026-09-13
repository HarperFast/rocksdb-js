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
						# Repeated per configuration because a configuration's msvs_settings
						# is merged last; see the target's own ExceptionHandling for why.
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
			# Exceptions must be enabled here, not just in the targets that include
			# gtest's headers. node's common.gypi turns them off by default, which
			# left GTEST_HAS_EXCEPTIONS 0 inside gtest-all.cc while the test TUs
			# (binding.gyp enables exceptions there) compiled the same headers with
			# it set to 1 - an ODR mismatch whose practical cost is that
			# HandleExceptionsInMethodIfSupported gets no catch blocks, so an
			# exception escaping a test body reaches main() unhandled. On Windows
			# that is std::terminate -> abort -> a silent exit code 0xC0000409 with
			# no failing-test line at all; with exceptions on, gtest reports it as a
			# normal test failure naming the test and the exception.
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
