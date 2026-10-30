#!/bin/sh
# Prints the Docker image tags for this pipeline, one per line; see "Versions and releases" in
# docs/development.md. With no argument, prints the tags of this commit's image. With `channels`,
# prints the channels a version tag moves to it; that needs git and the tags on origin.
set -eu

version_pattern='^[0-9]+\.[0-9]+\.[0-9]+(-dev\.[0-9]+)?$'

fail() {
	printf 'image tags: %s\n' "$*" >&2
	exit 1
}

# The newest of the versions on stdin; releases follow their development versions.
newest() {
	awk '
		function later(k, b, i) {
			for (i = 1; i <= 5; i++) if (k[i] != b[i]) return k[i] > b[i]
			return 0
		}
		/^[0-9]+\.[0-9]+\.[0-9]+(-dev\.[0-9]+)?$/ {
			n = split($0, p, /[.-]/)
			k[1] = p[1] + 0; k[2] = p[2] + 0; k[3] = p[3] + 0
			k[4] = n == 3; k[5] = n == 3 ? 0 : p[5] + 0
			if (best == "" || later(k, b)) {
				best = $0
				for (i = 1; i <= 5; i++) b[i] = k[i]
			}
		}
		END { print best }
	'
}

version=""
if [ -n "${CI_COMMIT_TAG:-}" ]; then
	version=${CI_COMMIT_TAG#v}
	printf '%s\n' "$version" | grep -Eq "$version_pattern" ||
		fail "tag $CI_COMMIT_TAG is not a version tag such as v0.0.2 or v0.0.2-dev.1"
fi

case ${1:-} in
"")
	exact=$version
	if [ -z "$exact" ] && [ -n "${CI_COMMIT_BRANCH:-}" ]; then
		exact=$(printf '%s' "$CI_COMMIT_BRANCH" | tr '/' '-')
		case $exact in
		latest | dev | sha-*) fail "branch $CI_COMMIT_BRANCH would overwrite the $exact image" ;;
		esac
		if printf '%s\n' "$exact" | grep -Eq "$version_pattern"; then
			fail "branch $CI_COMMIT_BRANCH would overwrite the $exact image"
		fi
	fi
	printf 'sha-%s\n' "$CI_COMMIT_SHA"
	[ -z "$exact" ] || printf '%s\n' "$exact"
	;;
channels)
	[ -n "$version" ] || fail "channels move only on version tags"
	remote=$(git ls-remote --tags --refs origin)
	versions=$(printf '%s\n%s\n' "$remote" "refs/tags/$CI_COMMIT_TAG" |
		awk '{ print $NF }' | sed -n 's|^refs/tags/v||p')
	releases=$(printf '%s\n' "$versions" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' || true)
	if [ "$(printf '%s\n' "$releases" | newest)" = "$version" ]; then
		printf 'latest\n'
	fi
	if [ "$(printf '%s\n' "$versions" | newest)" = "$version" ]; then
		printf 'dev\n'
	fi
	;;
*) fail "unknown mode $1" ;;
esac
