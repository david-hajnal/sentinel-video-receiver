#!/usr/bin/env bash
#
# install.sh - Idempotent installation script for sentinel_rtp_cam on Raspberry Pi OS
#
# Usage:
#   sudo ./install.sh
#
# Environment variables:
#   SENTINEL_VERSION    - Version to install (default: "latest")
#   SENTINEL_REPO       - GitHub repo (default: "kaszperek/sentinel-video-receiver")
#   SENTINEL_BASE_URL   - Base URL for artifacts (default: GitHub releases)
#   BUILD_FROM_SOURCE   - If "1", build from source instead of downloading binary
#
set -euo pipefail

# --- Configuration ---
readonly BINARY_NAME="sentinel_rtp_cam"
readonly INSTALL_DIR="/usr/local/bin"
readonly CONFIG_DIR="/etc/${BINARY_NAME}"
readonly STATE_DIR="/var/lib/${BINARY_NAME}"
readonly SERVICE_USER="sentinel"
readonly SERVICE_FILE="/etc/systemd/system/${BINARY_NAME}.service"

SENTINEL_VERSION="${SENTINEL_VERSION:-latest}"
SENTINEL_REPO="${SENTINEL_REPO:-david-hajnal/sentinel-video-receiver}"
SENTINEL_BASE_URL="${SENTINEL_BASE_URL:-https://github.com/${SENTINEL_REPO}/releases/download}"
BUILD_FROM_SOURCE="${BUILD_FROM_SOURCE:-0}"

# --- Colors for output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# --- Helper functions ---
log_info() {
    echo -e "${GREEN}[INFO]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

die() {
    log_error "$*"
    exit 1
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        die "This script must be run as root (use sudo)"
    fi
}

detect_arch() {
    local machine
    machine=$(uname -m)
    case "$machine" in
        armv7l)
            echo "armv7"
            ;;
        aarch64)
            echo "aarch64"
            ;;
        *)
            die "Unsupported architecture: $machine"
            ;;
    esac
}

install_dependencies() {
    log_info "Ensuring required dependencies are installed..."
    
    # Check if we need to install anything
    local need_install=0
    for cmd in curl tar sha256sum; do
        if ! command -v "$cmd" &>/dev/null; then
            need_install=1
            break
        fi
    done
    
    if [[ $need_install -eq 1 ]]; then
        log_info "Installing missing dependencies..."
        apt-get update -qq
        apt-get install -y curl ca-certificates coreutils tar
    else
        log_info "All required dependencies already present"
    fi
    
    # FFmpeg is required for the app
    if ! command -v ffmpeg &>/dev/null; then
        log_info "Installing ffmpeg (required for video processing)..."
        apt-get install -y ffmpeg
    fi
}

create_user() {
    if getent passwd "$SERVICE_USER" &>/dev/null; then
        log_info "User '$SERVICE_USER' already exists"
    else
        log_info "Creating system user '$SERVICE_USER'..."
        useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
    fi
}

create_directories() {
    log_info "Creating required directories..."
    
    install -d -m 755 "$INSTALL_DIR"
    install -d -m 750 -o root -g root "$CONFIG_DIR"
    install -d -m 750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$STATE_DIR"
    install -d -m 750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$STATE_DIR/clips"
}

download_binary() {
    local arch="$1"
    local version="$2"
    local tarball_name="${BINARY_NAME}-${version}-${arch}.tar.gz"
    
    # Handle "latest" by fetching latest release tag from GitHub
    if [[ "$version" == "latest" ]]; then
        log_info "Fetching latest release version from GitHub..."
        version=$(curl -fsSL "https://api.github.com/repos/${SENTINEL_REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"v([^"]+)".*/\1/')
        if [[ -z "$version" ]]; then
            log_error "Failed to fetch latest version from GitHub"
            return 1
        fi
        log_info "Latest version: $version"
        tarball_name="${BINARY_NAME}-${version}-${arch}.tar.gz"
    fi
    
    local download_url="${SENTINEL_BASE_URL}/v${version}/${tarball_name}"
    local checksum_url="${SENTINEL_BASE_URL}/v${version}/${tarball_name}.sha256"
    local temp_dir
    temp_dir=$(mktemp -d)
    local tarball_path="${temp_dir}/${tarball_name}"
    
    log_info "Downloading ${tarball_name} from GitHub releases..."
    log_info "URL: $download_url"
    
    if ! curl -fsSL -o "$tarball_path" "$download_url"; then
        rm -rf "$temp_dir"
        return 1
    fi
    
    # Verify checksum (always available from GitHub releases)
    log_info "Verifying checksum..."
    local expected_sha
    expected_sha=$(curl -fsSL "$checksum_url" | awk '{print $1}')
    if [[ -z "$expected_sha" ]]; then
        log_warn "Could not fetch checksum, skipping verification"
    else
        local actual_sha
        actual_sha=$(sha256sum "$tarball_path" | awk '{print $1}')
        
        if [[ "$expected_sha" != "$actual_sha" ]]; then
            log_error "Checksum verification failed!"
            log_error "Expected: $expected_sha"
            log_error "Got: $actual_sha"
            rm -rf "$temp_dir"
            return 1
        fi
        log_info "Checksum verified successfully"
    fi
    
    # Extract binary
    log_info "Extracting binary..."
    tar -xzf "$tarball_path" -C "$temp_dir"
    
    if [[ ! -f "${temp_dir}/${BINARY_NAME}" ]]; then
        log_error "Binary not found in tarball"
        rm -rf "$temp_dir"
        return 1
    fi
    
    # Install binary
    log_info "Installing binary to ${INSTALL_DIR}/${BINARY_NAME}..."
    install -m 755 "${temp_dir}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
    
    rm -rf "$temp_dir"
    return 0
}

build_from_source() {
    log_info "Building from source..."
    
    # Check if we're in the project directory
    if [[ ! -f "Cargo.toml" ]]; then
        log_info "Not in project directory, cloning repository..."
        local tmp_dir="/tmp/sentinel-video-receiver-$$"
        git clone "https://github.com/${SENTINEL_REPO}.git" "$tmp_dir"
        cd "$tmp_dir/sentinel_rtp_cam"
    fi
    
    # Install Rust if needed
    if ! command -v cargo &>/dev/null; then
        log_info "Installing Rust..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
        source "$HOME/.cargo/env"
    fi
    
    # Build
    log_info "Building release binary (this may take 15-30 minutes on Raspberry Pi)..."
    cargo build --release --bin app
    
    # Install
    log_info "Installing binary to ${INSTALL_DIR}/${BINARY_NAME}..."
    install -m 755 "target/release/app" "${INSTALL_DIR}/${BINARY_NAME}"
}

create_config_template() {
    local env_file="${CONFIG_DIR}/env"
    
    if [[ -f "$env_file" ]]; then
        log_warn "Config file already exists at $env_file - not overwriting"
        log_info "To reconfigure, edit $env_file manually"
        return
    fi
    
    log_info "Creating configuration template at $env_file..."
    
    cat > "$env_file" <<'EOF'
# Sentinel RTP Camera Agent Configuration
# Edit these values according to your setup

# Camera RTSP Configuration
RTSP_URL=rtsp://192.168.1.100:554/stream1
RTSP_HOST=192.168.1.100
RTSP_PORT=554
RTSP_USER=admin
RTSP_PASS=changeme

# ONVIF Motion Detection
ONVIF_HOST=192.168.1.100
ONVIF_PORT=80
ONVIF_USER=admin
ONVIF_PASS=changeme

# Debug options (optional)
# ONVIF_DEBUG=1
# ONVIF_DUMP_XML=1

# Server Configuration
SERVER_BASE_URL=http://your-server:8000
SERVER_BEARER_TOKEN=devtoken
CAMERA_ID=pi-cam-001

# Clip Recording
CLIP_DIR=/var/lib/sentinel_rtp_cam/clips
CLIP_PRE_ROLL_SECS=2
CLIP_POST_ROLL_SECS=3
CLIP_FPS=25
CLIP_STREAM_COPY=1

# Storage Management
CLIP_MAX_FILES=100
CLIP_MAX_AGE_SECS=604800
CLIP_MAX_TOTAL_BYTES=5368709120
CLIP_MAX_BYTES=52428800
CLIP_MAX_SECS=60

# Logging
RUST_LOG=info

# Version pinning (optional - uncomment to pin to specific version)
# SENTINEL_VERSION=1.0.0
EOF
    
    chmod 640 "$env_file"
    chown root:root "$env_file"
    
    log_warn "IMPORTANT: Edit $env_file with your camera credentials and server details"
}

install_systemd_service() {
    if [[ -f "$SERVICE_FILE" ]]; then
        log_info "Systemd service file already exists"
    else
        log_info "Installing systemd service file..."
        
        cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Sentinel RTP Camera Agent
Documentation=https://github.com/yourusername/sentinel-video-receiver
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$STATE_DIR

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$STATE_DIR
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictRealtime=true
RestrictNamespaces=true
LockPersonality=true

# Resource limits
LimitNOFILE=65536

# Environment
EnvironmentFile=$CONFIG_DIR/env

# Execution
ExecStart=$INSTALL_DIR/$BINARY_NAME

# Restart policy
Restart=on-failure
RestartSec=10
StartLimitInterval=200
StartLimitBurst=5

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$BINARY_NAME

[Install]
WantedBy=multi-user.target
EOF
        
        chmod 644 "$SERVICE_FILE"
    fi
    
    log_info "Reloading systemd daemon..."
    systemctl daemon-reload
}

enable_service() {
    log_info "Enabling $BINARY_NAME service..."
    systemctl enable "$BINARY_NAME"
}

main() {
    log_info "Starting sentinel_rtp_cam installation..."
    log_info "Version: $SENTINEL_VERSION"
    
    check_root
    
    local arch
    arch=$(detect_arch)
    log_info "Detected architecture: $arch"
    
    install_dependencies
    create_user
    create_directories
    
    # Install binary
    if [[ "$BUILD_FROM_SOURCE" == "1" ]]; then
        build_from_source
    else
        if ! download_binary "$arch" "$SENTINEL_VERSION"; then
            log_warn "Failed to download prebuilt binary"
            
            # Ask user if they want to build from source
            if [[ -t 0 ]]; then
                log_info "Would you like to build from source instead? (y/n)"
                read -r response
                if [[ "$response" =~ ^[Yy]$ ]]; then
                    BUILD_FROM_SOURCE=1
                    build_from_source
                else
                    die "Installation aborted. You can retry with: sudo BUILD_FROM_SOURCE=1 ./install.sh"
                fi
            else
                # Non-interactive mode - fail instead of auto-building
                log_error "Non-interactive mode: cannot prompt for source build"
                die "Installation failed. Retry with: sudo BUILD_FROM_SOURCE=1 ./install.sh"
            fi
        fi
    fi
    
    # Verify binary
    if [[ ! -x "${INSTALL_DIR}/${BINARY_NAME}" ]]; then
        die "Binary installation failed - ${INSTALL_DIR}/${BINARY_NAME} not found or not executable"
    fi
    
    log_info "Binary installed successfully: ${INSTALL_DIR}/${BINARY_NAME}"
    "${INSTALL_DIR}/${BINARY_NAME}" --version 2>/dev/null || log_warn "Binary doesn't support --version flag"
    
    create_config_template
    install_systemd_service
    enable_service
    
    log_info ""
    log_info "=========================================="
    log_info "Installation completed successfully!"
    log_info "=========================================="
    log_info ""
    log_info "Next steps:"
    log_info "  1. Edit configuration: sudo nano $CONFIG_DIR/env"
    log_info "  2. Start the service: sudo systemctl start $BINARY_NAME"
    log_info "  3. Check status: sudo systemctl status $BINARY_NAME"
    log_info "  4. View logs: sudo journalctl -u $BINARY_NAME -f"
    log_info ""
}

main "$@"
