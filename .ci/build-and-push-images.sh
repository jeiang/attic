#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt "2" ]]; then
	>&2 echo "Usage: $0 <image name> <tag1> ..."
	>&2 echo "Example: $0 ghcr.io/zhaofengli/attic main abcd123"
	exit 1
fi

work_dir=""

cleanup() {
	if [[ -n "${work_dir}" && -d "${work_dir}" ]]; then
		rm -rf "${work_dir}"
	fi
}
trap cleanup EXIT

image_name="$1"
tags=("${@:2}")

work_dir="$(mktemp -d -t attic-images.XXXXXXXXXX)"
manifest_spec="${work_dir}/manifest-tool.yaml"

# Do not inherit the host's containers configuration. In particular, recent
# versions of skopeo reject the v1 registries.conf installed on GitHub runners.
export XDG_CONFIG_HOME="${work_dir}/config"
mkdir -p "${XDG_CONFIG_HOME}/containers"
cat >"${XDG_CONFIG_HOME}/containers/registries.conf" <<'EOF'
unqualified-search-registries = []
EOF

declare -a digests

emit_header() {
	echo "image: ${image_name}"
	echo "tags:"
	for tag in "${tags[@]}"; do
		echo "- ${tag}"
	done
	echo "manifests:"
}

push_digest() {
	source_image="docker-archive:$1"
	digest="$(skopeo inspect "${source_image}" | jq -r .Digest)"
	target_image="docker://${image_name}@${digest}"

	>&2 echo "${source_image} ▸ ${target_image}"
	>&2 skopeo copy --insecure-policy "${source_image}" "${target_image}"

	echo -n "- "
	skopeo inspect "${source_image}" | \
		jq '{platform: {architecture: .Architecture, os: .Os}, image: ($image_name + "@" + .Digest)}' \
		--arg image_name "${image_name}"
}

>>"${manifest_spec}" emit_header

if [[ -n "${ATTIC_IMAGE_AMD64:-}" && -n "${ATTIC_IMAGE_ARM64:-}" ]]; then
	>>"${manifest_spec}" push_digest "${ATTIC_IMAGE_AMD64}"
	>>"${manifest_spec}" push_digest "${ATTIC_IMAGE_ARM64}"
elif [[ -n "${ATTIC_IMAGE_AMD64:-}" || -n "${ATTIC_IMAGE_ARM64:-}" ]]; then
	>&2 echo "ATTIC_IMAGE_AMD64 and ATTIC_IMAGE_ARM64 must be set together"
	exit 1
else
	nix build .#attic-server-image .#attic-server-image-aarch64 --no-link -L --print-out-paths | \
	while read -r output; do
		>>"${manifest_spec}" push_digest "${output}"
	done
fi

>&2 echo "----------"
>&2 echo "Generated manifest-tool spec:"
>&2 echo "----------"
cat "${manifest_spec}"
>&2 echo "----------"

manifest-tool push from-spec "${manifest_spec}"
