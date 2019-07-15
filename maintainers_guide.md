# Maintainer's Guide

This document describes tools, tasks and workflow that one needs to be familiar with in order to effectively maintain
this project. If you use this package within your own software as is but don't plan on modifying it, this guide is
**not** for you.

## Tasks

### Testing

Run unit tests locally using `vendor/bin/hacktest tests/`. Tests are also run automatically on all branches using [Travis CI](https://travis-ci.org/slackhq/vscode-hack).

### Releasing

To push a new release, follow these steps:

1. Make sure the `master` branch is up to date with all changes and has been tested, and the Travis build is passing.
2. Merge a new commit with the following changes:
   - Update the version in `package.json` by following the versioning guide below
   - Add a description of all changes since the last release in `CHANGELOG.md`
   - Add or update the "Latest releases" section in `README.md` with release highlights
3. Create a new GitHub release:
   - Releases should always target the `master` branch
   - Tag version and release title should both be in the format "v1.2.3", with the version matching the value in `composer.json`
   - Copy your new section from `CHANGELOG.md` in the release description

## Workflow

### Versioning and Tags

Like other Hacklang projects, versions of SQLFake are tagged to versions of HHVM that they support. The MAJOR and MINOR versions should match HHVM version numbers, and the PATCH version can be used for SQLFake bug fixes and improvements.

Releases are tagged in git and published on the repository releases page on GitHub.

### Branches

All development should happen in feature branches. `master` should be ready for quick patching and publishing at all times.

### Issue Management

Labels are used to run issues through an organized workflow. Here are the basic definitions:

- `bug`: A confirmed bug report. A bug is considered confirmed when reproduction steps have been
  documented and the issue has been reproduced.
- `enhancement` or `feature request`: A feature request for something this package might not already do.
- `docs`: An issue that is purely about documentation work.
- `tests`: An issue that is purely about testing work.
- `needs feedback`: An issue that may have claimed to be a bug but was not reproducible, or was otherwise missing some information.
- `discussion`: An issue that is purely meant to hold a discussion. Typically the maintainers are looking for feedback in this issues.
- `question`: An issue that is like a support request because the user's usage was not correct.
- `semver:major|minor|patch`: Metadata about how resolving this issue would affect the version number.
- `security`: An issue that has special consideration for security reasons.
- `good first contribution`: An issue that has a well-defined relatively-small scope, with clear expectations. It helps when the testing approach is also known.
- `duplicate`: An issue that is functionally the same as another issue. Apply this only if you've linked the other issue by number.
- `external`: An issue that is caused by an external dependency and cannot be fixed here.

**Triage** is the process of taking new issues that aren't yet "seen" and marking them with a basic
level of information with labels. An issue should have **one** of the following labels applied:
`bug`, `enhancement`, `question`, `needs feedback`, `docs`, `tests`, or `discussion`.

Issues are closed when a resolution has been reached. If for any reason a closed issue seems
relevant once again, reopening is great and better than creating a duplicate issue.

## Everything else

When in doubt, find the other maintainers and ask.
