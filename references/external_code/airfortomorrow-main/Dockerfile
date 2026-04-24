
# FROM --platform=linux/amd64 python:3.10-slim
# FROM python:3.10-slim
FROM python:3.10.17-slim 

# Set environment variables early
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies (replaces the OS detection part)
RUN apt-get update && apt-get install -y \
    # Basic tools
    bash \
    curl \
    jq \
    # GDAL dependencies
    gdal-bin \
    libgdal-dev \
    # ecCodes dependencies  
    libeccodes0 \
    libeccodes-dev \
    libeccodes-tools \
    # Build tools (might be needed for some Python packages)
    build-essential \
    # Python dev headers (needed for GDAL Python bindings)
    python3-dev \
    # # Added these essential geospatial libraries:
    # libgeos-dev \
    # libgeos-c1v5 \
    # libproj-dev \
    # proj-bin \
    # proj-data \
    # cmake \
    # ninja-build \
    # pkg-config \
    # Plus additional libraries for compilation:
    libspatialindex-dev \
    libudunits2-dev \
    libffi-dev \
    libssl-dev \
    # Clean up
    && rm -rf /var/lib/apt/lists/* \
    && ldconfig


ENV PYTHONPATH=/app:$PYTHONPATH

# Set GDAL environment variables
ENV LD_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH}"
ENV GDAL_LIBRARY_PATH="/usr/lib/libgdal.so"
ENV GDAL_DATA="/usr/share/gdal"

# Use ecCodes definitions/samples bundled with eccodeslib.
# Pinning to system definitions can mismatch the Python library version and segfault.
ENV ECCODES_DEFINITION_PATH="/MEMFS/definitions"
ENV ECCODES_SAMPLES_PATH="/MEMFS/samples"

# Set GDAL optimization variables (prevents segfaults)
ENV GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR
ENV CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".nc,.grb,.grib2"
ENV GDAL_MAX_DATASET_POOL_SIZE=100
ENV GDAL_CACHEMAX=512
ENV CPL_TMPDIR="/tmp"
ENV GDAL_HTTP_TIMEOUT=30
ENV GDAL_HTTP_CONNECTTIMEOUT=10

# Set working directory
WORKDIR /app

# Copy requirements first (for better Docker layer caching)
COPY requirements.txt .

RUN python -m pip install --upgrade pip setuptools wheel

# Upgrade pip and install Python dependencies
RUN python -m pip install --no-cache-dir -r requirements.txt

# Install GDAL FIRST
RUN GDAL_VERSION=$(gdal-config --version) && \
pip install --no-cache-dir GDAL==$GDAL_VERSION

# Install yq (YAML processor)
RUN pip install --no-cache-dir yq

# Try to install eccodes from source if binary wheels fail
RUN echo "Attempting to install eccodes from source..." && \
    (pip install --no-cache-dir eccodes==2.43.0 || \
     pip install --no-cache-dir --no-binary=eccodes eccodes || \
     echo "eccodes installation failed, proceeding without it")

# Install cfgrib after eccodes
RUN echo "Installing cfgrib..." && \
    (pip install --no-cache-dir cfgrib || \
     echo "cfgrib installation failed, some GRIB functionality may not work")


# Attempt to fix ecCodes Python packages if needed
RUN python -c "import eccodes; print('✅ eccodes import successful')" || \
    (echo "Fixing ecCodes packages..." && \
     pip uninstall -y cfgrib eccodes gribapi || true && \
     pip install --no-cache-dir eccodes cfgrib)


# Create necessary directories
RUN mkdir -p /app/assets/dem \
    /app/assets/landcover \
    /app/assets/worldpop \
    /app/data/raw/firms/historical \
    /app/config \
    /tmp

# Copy application code
COPY . .

# Make all shell scripts executable
RUN find . -name "*.sh" -exec chmod +x {} \;

# Verify installations
RUN set -eu; \
    echo "Verifying installations..." && \
    python -c "from osgeo import gdal; print('GDAL version:', gdal.__version__)" && \
    python -c "import eccodes; print('ecCodes import: OK')" && \
    python -c "import earthkit.data; print('earthkit import: OK')" && \
    python -c "import cfgrib; print('cfgrib import: OK')" && \
    grib_dump -V && \
    jq --version && \
    yq --version

# Build-time smoke test for ecCodes GRIB sample creation path.
# This fails fast on definition/library mismatches that can cause segfaults at runtime.
RUN set -eu; \
    echo "Running ecCodes GRIB smoke test..." && \
    python - <<'PY'
import faulthandler

faulthandler.enable()

import eccodes
import gribapi

gid = gribapi.grib_new_from_samples("GRIB2")
gribapi.grib_release(gid)

print("ecCodes API:", eccodes.codes_get_api_version())
print("ecCodes defs:", eccodes.codes_definition_path())
print("ecCodes samples:", eccodes.codes_samples_path())
print("ecCodes GRIB sample smoke test: OK")
PY



# Copy and set up entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh



# Use custom entrypoint that shows available commands
ENTRYPOINT ["/entrypoint.sh"]
CMD []
