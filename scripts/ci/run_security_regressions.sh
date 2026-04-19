#!/usr/bin/env bash
set -euo pipefail

go test ./internal/proxy \
  -run '^(TestHardening_|TestTenantScoping_|TestTailHardening_(RejectsBrowserOriginsByDefault|AllowsConfiguredOrigin)|TestPeerCacheMiddleware|TestEnsureWritableSnapshotPath_CreatesTargetFile|TestNew_FailsFastOnUnwritable(PatternsPersistPath|LabelValuesPersistPath))$' \
  -count=1

go test -v -tags=e2e -timeout="${E2E_SECURITY_TIMEOUT:-150s}" -count=1 \
  -run '^(TestSetup_IngestLogs|TestFeature_Multitenancy_.*|TestFeature_AdminDebugEndpoints_DefaultClosed|TestFeature_QueryAnalytics_Endpoint|TestFeature_Tail_BrowserOriginRejectedByDefault|TestFeature_SecurityHeaders)$' \
  ./test/e2e-compat/
