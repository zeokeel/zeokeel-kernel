# Security Policy

## Supported versions

Security fixes are applied to the latest released minor line (`v0.x` during the pre-1.0 phase).
Older tags are not maintained unless explicitly stated in the release notes.

## Reporting a vulnerability

**Do not open a public issue.** Report privately through GitHub Security Advisories:

<https://github.com/zeokeel/zeokeel-kernel/security/advisories/new>

Please include: affected version/tag, a minimal reproducer, impact assessment, and any suggested fix.
We aim to acknowledge reports within a few business days and will coordinate disclosure after a fix is
available.

## Scope and threat model

Zeokeel Kernel runs all modules in the same process and treats them as **trusted code**. The kernel
provides panic containment at its own call boundaries, but a module can still crash the process through
fatal runtime errors, OOM, unsafe/cgo, `os.Exit`, or a panic in a goroutine that it spawned without
`module.SafeGo`. Reports that assume a sandbox/isolation boundary are out of scope; process isolation
(L1) is a host responsibility. See `docs/COMPILE-TIME-REGISTRY.md` §6.

The kernel is standard-library only; dependency supply-chain reports should target the Go toolchain or
host module set rather than this repository.
