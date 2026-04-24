#!/usr/bin/env bash
set -euo pipefail

# This simplified setup.sh only handles runtime-specific configurations
# Most setup has been moved to the Dockerfile

echo "🚀 Docker runtime setup..."

# Set the base directory to the project root
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
cd "$BASE_DIR"

# Check if we're running in Docker (optional - for debugging)
if [ -f /.dockerenv ]; then
    echo "→ Running inside Docker container"
else
    echo "→ Running outside Docker container"
fi

# Verify critical components are working
echo "🔍 Verifying system components..."

# Test GDAL
if python -c "from osgeo import gdal; print('GDAL OK')" 2>/dev/null; then
    echo "✅ GDAL: Working"
else
    echo "❌ GDAL: Failed"
    exit 1
fi

# Test ecCodes
if python -c "import eccodes, cfgrib; print('ecCodes OK')" 2>/dev/null; then
    echo "✅ ecCodes: Working"
else
    echo "❌ ecCodes: Failed"
    exit 1
fi

# Test configuration access (if config file exists)
if [ -f "config/config.yaml" ]; then
    if yq '.system.geographic.buffers.firms' config/config.yaml >/dev/null 2>&1; then
        echo "✅ Configuration: Accessible"
    else
        echo "⚠️  Configuration: May have issues"
    fi
else
    echo "⚠️  Configuration file not found (config/config.yaml)"
fi

# check that all data folders exist
mkdir -p /app/data/{raw,processed,features,models,logs}
mkdir -p /app/assets

# Validate required bootstrap files from checksum manifest
echo "📥 Validating required bootstrap files..."

CHECKSUM_MANIFEST="assets/checksums/bootstrap_assets_manifest.tsv"

if [ ! -f "$CHECKSUM_MANIFEST" ]; then
    echo "❌ Missing checksum manifest: $CHECKSUM_MANIFEST"
    echo "💡 Ensure repository assets are present and run: git lfs pull"
    exit 1
fi

get_file_size() {
    local file="$1"
    if stat -c%s "$file" >/dev/null 2>&1; then
        stat -c%s "$file"
    else
        stat -f%z "$file"
    fi
}

get_sha256() {
    local file="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$file" | awk '{print $1}'
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 "$file" | awk '{print $NF}'
    else
        echo "UNAVAILABLE"
    fi
}

missing_count=0
size_mismatch_count=0
checksum_mismatch_count=0
validated_count=0

while IFS=$'\t' read -r file expected_size expected_sha256; do
    # Skip comments/blank lines in the manifest
    if [[ -z "$file" || "$file" == \#* ]]; then
        continue
    fi

    if [ ! -f "$file" ]; then
        echo "❌ Missing required file: $file"
        missing_count=$((missing_count + 1))
        continue
    fi

    actual_size="$(get_file_size "$file")"
    if [ "$actual_size" != "$expected_size" ]; then
        echo "❌ Size mismatch for $file (expected: $expected_size, got: $actual_size)"
        size_mismatch_count=$((size_mismatch_count + 1))
        continue
    fi

    actual_sha256="$(get_sha256 "$file")"
    if [ "$actual_sha256" = "UNAVAILABLE" ]; then
        echo "❌ Could not compute SHA256 for $file (sha256sum/shasum/openssl not available)"
        checksum_mismatch_count=$((checksum_mismatch_count + 1))
        continue
    fi

    if [ "$actual_sha256" != "$expected_sha256" ]; then
        echo "❌ Checksum mismatch for $file"
        echo "   expected: $expected_sha256"
        echo "   got:      $actual_sha256"
        checksum_mismatch_count=$((checksum_mismatch_count + 1))
        continue
    fi

    echo "✅ Verified $file"
    validated_count=$((validated_count + 1))
done < "$CHECKSUM_MANIFEST"

if [ "$missing_count" -gt 0 ] || [ "$size_mismatch_count" -gt 0 ] || [ "$checksum_mismatch_count" -gt 0 ]; then
    echo ""
    echo "❌ Bootstrap file validation failed"
    echo "   Missing files: $missing_count"
    echo "   Size mismatches: $size_mismatch_count"
    echo "   Checksum mismatches: $checksum_mismatch_count"
    echo "💡 Run 'git lfs pull' and retry."
    exit 1
fi

echo "📦 Bootstrap file validation complete ($validated_count files verified)."

# Display system info for debugging
echo "🔧 System information:"
echo "→ Python: $(python --version)"
echo "→ GDAL: $(python -c 'from osgeo import gdal; print(gdal.__version__)' 2>/dev/null || echo 'Not available')"
echo "→ Working directory: $(pwd)"
echo "→ Available memory: $(free -h | grep '^Mem:' | awk '{print $2}' 2>/dev/null || echo 'Unknown')"

echo "✅ Runtime setup completed successfully!"
echo "🚀 System is ready for air quality prediction pipeline"
