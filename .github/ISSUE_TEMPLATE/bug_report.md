---
name: Bug report
about: Report a problem with lance-go
title: ""
labels: bug
assignees: ""
---

## Version

- lance-go version / commit: <!-- e.g. v0.1.0, or a commit SHA -->
- Installation method: <!-- prebuilt artifacts (scripts/download-artifacts.sh) or built from source (make rust) -->
- Go version: <!-- go version -->
- Rust version (if built from source): <!-- rustc --version -->
- OS / architecture: <!-- e.g. linux/amd64, darwin/arm64 -->

## Minimal repro

<!-- A minimal, runnable Go snippet (or a link to one) that reproduces the
issue. Include how the dataset was created if that's relevant. -->

```go

```

## Expected behavior

<!-- What you expected to happen. -->

## Actual behavior

<!-- What actually happened. Include the full error message and, if
applicable, whether it came back as a wrapped sentinel error
(errors.Is(err, lance.ErrXxx)). -->

## Logs / output

<!-- Any relevant panic output, stack traces, or logs. Use a code block. -->

```

```
