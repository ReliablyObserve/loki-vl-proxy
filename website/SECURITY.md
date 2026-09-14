# Temporary image-size patch

image-size 2.0.2 has no published patched release for
[GHSA-w3rx-r6r6-pgpr](https://github.com/advisories/GHSA-w3rx-r6r6-pgpr)
and [GHSA-5p2g-fcmc-qvqq](https://github.com/advisories/GHSA-5p2g-fcmc-qvqq).
These affect build-time image parsing, not the Go proxy runtime.

`scripts/harden-image-size.cjs` adds progress and header-length checks to all
20 installed CJS/ESM bundles containing the affected code. ICNS entries must
advance by at least their header size. BMFF boxes must contain their header;
size zero is interpreted as extending to EOF, preserving that valid format
convention while ensuring JXL/HEIF loops advance. Valid image parsing remains
available. Before/after SHA-256 hashes and the exact package version make the
patch fail closed if dependency contents change.

The patch runs after installation and before builds, including installs using
`--ignore-scripts`. Run `npm run test:security` to verify malformed ICNS,
JXL and HEIF cases in timeout-isolated subprocesses through both public module
entry points, plus valid PNG/ICNS and size-zero BMFF handling.

The npm advisory scanner will continue to flag version 2.0.2: local patching does
not change registry advisory metadata. Do not describe this as a clean npm audit
or add a blanket audit suppression. Maintainers must reassess the patch when
updating Docusaurus/image-size and replace it with a verified upstream release
when available. Remove both the patch and manifest once those regression tests
pass on the upstream fix. Recheck advisory status before each release.
