#!/bin/sh
# Checks the version line, PREVIOUS_RELEASE in lyra-db/src/migration.rs, web.rev, and the releases
# in versions.json; see "Versions and releases" in docs/development.md. Needs the full history, git
# and jq.
set -eu

failed=0
fail() {
	printf 'release check: %s\n' "$*" >&2
	failed=1
}

workspace_version() {
	git show "$1:Cargo.toml" | awk '
		/^\[/ { in_package = ($0 == "[workspace.package]") }
		in_package && /^version *=/ { gsub(/^version *= *"|"$/, ""); print; exit }
	'
}

# Paths whose changes reach the server a release runs on.
server_changed() {
	! git diff --quiet "$1" "$2" -- Cargo.toml Cargo.lock 'harmony-*' 'lyra-*' \
		':(exclude)lyra-docs' ':(exclude)lyra-harmony-test'
}

plugin_manifest() {
	git show "$1:plugins/$2/plugin.json" | jq -S 'del(.version, .lyra)'
}

# Whether a plugin changed, apart from its tests and its manifest's version fields.
plugin_changed() {
	if [ -n "$(git diff --name-only "$1" "$2" -- "plugins/$3" \
		":(exclude)plugins/$3/tests" ":(exclude)plugins/$3/plugin.json")" ]; then
		return 0
	fi
	[ "$(plugin_manifest "$1" "$3")" != "$(plugin_manifest "$2" "$3")" ]
}

head_version=$(workspace_version HEAD)
if [ -n "${CI_COMMIT_TAG:-}" ]; then
	printf '%s\n' "$head_version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' ||
		fail "release tag $CI_COMMIT_TAG is on version $head_version, which has a pre-release"
	[ "$CI_COMMIT_TAG" = "v$head_version" ] ||
		fail "release tag $CI_COMMIT_TAG does not match version $head_version"
else
	printf '%s\n' "$head_version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+-dev\.[0-9]+$' ||
		fail "version $head_version is not a development version such as 0.0.2-dev.0"
fi

# Whether release $1 precedes version $2, ignoring $2's pre-release: 0.0.2-dev.N follows every
# release before 0.0.2.
release_before() {
	printf '%s %s\n' "$1" "${2%%-*}" | awk '{
		split($1, a, "."); split($2, b, ".")
		for (i = 1; i <= 3; i++) if (a[i] != b[i]) exit !(a[i] + 0 < b[i] + 0)
		exit 1
	}'
}

previous_release=""
for tag in $(git tag -l 'v*' --sort=-v:refname); do
	version=${tag#v}
	printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || continue
	if release_before "$version" "$head_version"; then
		previous_release=$version
		break
	fi
done
expected=None
[ -z "$previous_release" ] || expected="Some(\"$previous_release\")"
declared=$(git show HEAD:lyra-db/src/migration.rs |
	sed -n 's/^pub(crate) const PREVIOUS_RELEASE: Option<&str> = \(.*\);$/\1/p')
[ "$declared" = "$expected" ] ||
	fail "PREVIOUS_RELEASE is ${declared:-missing} but should be $expected, the newest release before $head_version"

web_rev=$(git show HEAD:web.rev)
printf '%s\n' "$web_rev" | grep -Eq '^[0-9a-f]{40}$' && [ "${#web_rev}" -eq 40 ] ||
	fail "web.rev does not hold a single full lyra-web commit id"

entries=$(jq -r '.[] | "\(.commit) \(.lyra)"' versions.json)
newer=""
newer_lyra=""
printf '%s\n' "$entries" | {
	while read -r commit lyra; do
		[ -n "$commit" ] || continue
		if ! git merge-base --is-ancestor "$commit" HEAD 2>/dev/null; then
			fail "release $commit is not in this branch's history"
			newer=""
			continue
		fi
		version=$(workspace_version "$commit")
		[ "$lyra" = "$version" ] ||
			fail "release $commit is offered to $lyra but has version $version"

		if [ -n "$newer" ]; then
			if server_changed "$commit" "$newer" && [ "$newer_lyra" = "$lyra" ]; then
				fail "release $newer changes the server since $commit but keeps version $lyra"
			fi
			for manifest in $(git ls-tree --name-only "$newer" plugins/ | sed 's|$|/plugin.json|'); do
				plugin=${manifest#plugins/}
				plugin=${plugin%/plugin.json}
				git cat-file -e "$commit:$manifest" 2>/dev/null || continue
				git cat-file -e "$newer:$manifest" 2>/dev/null || continue
				plugin_changed "$commit" "$newer" "$plugin" || continue
				before=$(git show "$commit:$manifest" | jq -r .version)
				after=$(git show "$newer:$manifest" | jq -r .version)
				[ "$before" != "$after" ] ||
					fail "plugin $plugin changed between $commit and $newer but keeps version $after"
			done
		fi
		newer=$commit
		newer_lyra=$lyra
	done
	exit "$failed"
} || failed=1

exit "$failed"
