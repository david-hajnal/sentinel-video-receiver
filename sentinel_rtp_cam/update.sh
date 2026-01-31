#!/usr/bin/env bash
#
# update.sh - Safe, idempotent update script for sentinel_rtp_cam
#
# Usage:
#   sudo ./update.sh [version] [--dry-run]
#
# Examples:
#   sudo ./update.sh latest
#   sudo ./update.sh v0.2.0
#   sudo ./update.sh --dry-run
#
# Environment variables:
#   SENTINEL_VERSION    - Version to update to (default: "latest")
#   SENTINEL_REPO       - GitHub repo (default: "kaszperek/sentinel-video-receiver")
#   SENTINEL_BASE_URL   - Base URL for artifacts (default: GitHub releases)
#
set -euo pipefail

# --- Configuration ---
readonly BINARY_NAME="sentinel_rtp_cam"
readonly FORWARD_BINARY_NAME="sentinel_rtp_cam_forward"
readonly INSTALL_DIR="/usr/local/bin"
readonly CONFIG_DIR="/etc/${BINARY_NAME}"
readonly STATE_DIR="/var/lib/${BINARY_NAME}"
readonly SERVICE_NAME="${BINARY_NAME}"
readonly FORWARD_SERVICE_NAME="${FORWARD_BINARY_NAME}"

SENTINEL_VERSION="${SENTINEL_VERSION:-latest}"
SENTINEL_REPO="${SENTINEL_REPO:-david-hajnal/sentinel-video-receiver}"
SENTINEL_BASE_URL="${SENTINEL_BASE_URL:-https://github.com/${SENTINEL_REPO}/releases/download}"
DRY_RUN=0

# --- Colors for output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

log_dry() {
    echo -e "${BLUE}[DRY-RUN]${NC} $*"
}

die() {
    log_error "$*"
    exit 1
}

check_root() {
    echo "Check for root..."
    if [[ $EUID -ne 0 ]]; then
        die "This script must be run as root (use sudo)"
    fi
}

detect_arch() {
    local machine
    machine=$(uname -m)
    case "$machine" in
        armv7l) echo "armv7" ;;
        aarch64) echo "aarch64" ;;
        *) die "Unsupported architecture: $machine" ;;
    esac
}

get_installed_version() {

   # local binary="${INSTALL_DIR}/${BINARY_NAME}"
   # if [[ ! -x "$binary" ]]; then
   #     log_info "No existing binary found at $binary"
   #     echo "none"
   #     return
   # fi

   # log_info "Checking version of installed binary..."
    # Try to get version from binary (assuming it supports --version)
   # local version
   # version=$("$binary" --version 2>/dev/null | head -n1 | awk '{print $NF}') || echo "unknown"
   # echo "$version"
echo 1
}

download_and_verify() {
    local arch="$1"
    local version="$2"
    local output_path="$3"
    local output_forward_path="$4"

    # Strip 'v' prefix if present to normalize version
    version="${version#v}"

    # Handle "latest" by fetching latest release tag from GitHub
    if [[ "$version" == "latest" ]]; then
        log_info "Fetching latest release version from GitHub API..."
        log_info "API endpoint: https://api.github.com/repos/${SENTINEL_REPO}/releases/latest"
        version=$(curl -fsSL --max-time 30 "https://api.github.com/repos/${SENTINEL_REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"v([^"]+)".*/\1/')
        if [[ -z "$version" ]]; then
            log_error "Failed to fetch latest version from GitHub API"
            log_error "Check releases manually: https://github.com/${SENTINEL_REPO}/releases"
            return 1
        fi
        log_info "✓ Latest version resolved: v$version"
    else
        log_info "Using specified version: v$version"
    fi

    local tarball_name="${BINARY_NAME}-${version}-${arch}.tar.gz"
    local download_url="${SENTINEL_BASE_URL}/v${version}/${tarball_name}"
    local checksum_url="${SENTINEL_BASE_URL}/v${version}/${tarball_name}.sha256"
    local temp_dir
    temp_dir=$(mktemp -d)
    local tarball_path="${temp_dir}/${tarball_name}"

    log_info "Downloading ${tarball_name}..."
    log_info "From: $download_url"
    log_info "To: $tarball_path"
    log_info "Timeout: 300 seconds"

    if ! curl -fL --progress-bar --max-time 300 -o "$tarball_path" "$download_url"; then
        log_error "Download failed!"
        log_error "Check if release exists: https://github.com/${SENTINEL_REPO}/releases/tag/v${version}"
        log_error "Check network connectivity and GitHub status"
        rm -rf "$temp_dir"
        return 1
    fi

    log_info "✓ Download completed: $(du -h "$tarball_path" | cut -f1)"

    # Verify checksum (always available from GitHub releases)
    log_info "Verifying SHA256 checksum..."
    log_info "Checksum URL: $checksum_url"
    local expected_sha
    expected_sha=$(curl -fsSL --max-time 30 "$checksum_url" | awk '{print $1}')
    if [[ -z "$expected_sha" ]]; then
        log_warn "Could not fetch checksum file, skipping verification"
        log_warn "This is not recommended - binary may be corrupted"
    else
        log_info "Computing SHA256 of downloaded file..."
        local actual_sha
        actual_sha=$(sha256sum "$tarball_path" | awk '{print $1}')

        log_info "Expected: $expected_sha"
        log_info "Actual:   $actual_sha"

        if [[ "$expected_sha" != "$actual_sha" ]]; then
            log_error "✗ Checksum verification FAILED!"
            log_error "The downloaded file is corrupted or tampered with"
            rm -rf "$temp_dir"
            return 1
        fi
        log_info "✓ Checksum verified successfully"
    fi

    # Extract binary
    log_info "Extracting tarball..."
    log_info "Archive contents:"
    tar -tzf "$tarball_path" | head -n 10

    if ! tar -xzf "$tarball_path" -C "$temp_dir"; then
        log_error "Failed to extract tarball"
        rm -rf "$temp_dir"
        return 1
    fi

    log_info "✓ Extraction completed"

    if [[ ! -f "${temp_dir}/${BINARY_NAME}" ]]; then
        log_error "Binary '$BINARY_NAME' not found in tarball!"
        log_error "Contents of extracted archive:"
        ls -lah "$temp_dir"
        rm -rf "$temp_dir"
        return 1
    fi

    log_info "Binary found: ${temp_dir}/${BINARY_NAME}"
    log_info "Binary size: $(du -h "${temp_dir}/${BINARY_NAME}" | cut -f1)"
    log_info "Binary type: $(file "${temp_dir}/${BINARY_NAME}" 2>/dev/null || echo 'unknown')"

    # Move main binary to output path
    log_info "Moving binary to: $output_path"
    mv "${temp_dir}/${BINARY_NAME}" "$output_path"
    chmod 755 "$output_path"

    log_info "✓ Binary installed at $output_path"

    # Optional forward binary
    if [[ -f "${temp_dir}/${FORWARD_BINARY_NAME}" ]]; then
        log_info "Forward binary found: ${temp_dir}/${FORWARD_BINARY_NAME}"
        log_info "Forward binary size: $(du -h "${temp_dir}/${FORWARD_BINARY_NAME}" | cut -f1)"
        log_info "Moving forward binary to: $output_forward_path"
        mv "${temp_dir}/${FORWARD_BINARY_NAME}" "$output_forward_path"
        chmod 755 "$output_forward_path"
        log_info "✓ Forward binary installed at $output_forward_path"
    else
        log_warn "Forward binary '${FORWARD_BINARY_NAME}' not found in tarball; skipping"
    fi

    rm -rf "$temp_dir"
    return 0
}

create_backup_binary() {
    local binary_name="$1"
    local current="${INSTALL_DIR}/${binary_name}"
    local backup="${INSTALL_DIR}/${binary_name}.prev"

    if [[ ! -f "$current" ]]; then
        log_warn "No current binary to back up (fresh install)"
        return
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        log_dry "Would back up: $current -> $backup"
        return
    fi

    log_info "Creating backup of current binary..."
    log_info "Current: $current ($(du -h "$current" | cut -f1))"
    log_info "Backup:  $backup"

    if cp -f "$current" "$backup"; then
        log_info "✓ Backup created successfully"
    else
        log_error "Failed to create backup!"
        return 1
    fi
}

install_new_binary_to() {
    local new_binary="$1"
    local target="$2"

    if [[ $DRY_RUN -eq 1 ]]; then
        log_dry "Would install: $new_binary -> $target"
        return
    fi

    log_info "Installing new binary..."
    log_info "Source: $new_binary"
    log_info "Target: $target"

    if ! mv -f "$new_binary" "$target"; then
        log_error "Failed to move binary to install location!"
        return 1
    fi

    chmod 755 "$target"

    log_info "✓ New binary installed"
    log_info "Verifying installation..."

    if [[ ! -x "$target" ]]; then
        log_error "Binary is not executable after installation!"
        return 1
    fi

    log_info "Binary permissions: $(ls -l "$target" | awk '{print $1, $3, $4}')"
}

restart_service() {
    if [[ $DRY_RUN -eq 1 ]]; then
        log_dry "Would restart service: $SERVICE_NAME"
        return
    fi

    log_info "Stopping $SERVICE_NAME service..."
    systemctl stop "$SERVICE_NAME" || true

    log_info "Starting $SERVICE_NAME service..."
    if ! systemctl start "$SERVICE_NAME"; then
        log_error "Failed to start service"
        log_error "Checking logs:"
        journalctl -u "$SERVICE_NAME" -n 20 --no-pager
        return 1
    fi

    log_info "Service restarted successfully"

    if systemctl list-unit-files --type=service | grep -q "^${FORWARD_SERVICE_NAME}\\.service"; then
        log_info "Restarting $FORWARD_SERVICE_NAME service..."
        if ! systemctl restart "$FORWARD_SERVICE_NAME"; then
            log_warn "Failed to restart $FORWARD_SERVICE_NAME"
        else
            log_info "$FORWARD_SERVICE_NAME restarted successfully"
        fi
    fi
}

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --dry-run)
                DRY_RUN=1
                log_info "Running in DRY-RUN mode (no changes will be made)"
                shift
                ;;
            -*)
                die "Unknown option: $1"
                ;;
            *)
                # Accept version as positional argument
                if [[ -z "${SENTINEL_VERSION_SET:-}" ]]; then
                    SENTINEL_VERSION="$1"
                    SENTINEL_VERSION_SET=1
                    shift
                else
                    die "Unknown argument: $1"
                fi
                ;;
        esac
    done

    log_info "Starting sentinel_rtp_cam update..."
    log_info "Target version: $SENTINEL_VERSION"

    check_root
    echo "Root check passed."
    # Check if service is installed
    if [[ ! -f "${INSTALL_DIR}/${BINARY_NAME}" ]]; then
        die "Service not installed. Install the binary + systemd unit first (see README_DEPLOY.md)."
    fi

    local current_version
    current_version=$(get_installed_version)
    log_info "Current version: $current_version"

    if [[ "$current_version" == "$SENTINEL_VERSION" && "$SENTINEL_VERSION" != "latest" ]]; then
        log_info "Already running target version $SENTINEL_VERSION"
        exit 0
    fi

    local arch
    arch=$(detect_arch)
    log_info "Architecture: $arch"

    # Download new binaries to temporary location
    local new_binary="${INSTALL_DIR}/${BINARY_NAME}.new"
    local new_forward_binary="${INSTALL_DIR}/${FORWARD_BINARY_NAME}.new"

    if [[ $DRY_RUN -eq 1 ]]; then
        log_dry "Would download version $SENTINEL_VERSION for $arch"
        log_dry "Would install to: $new_binary"
        log_dry "Would install to: $new_forward_binary"
        log_info "Dry-run completed successfully"
        exit 0
    fi

    if ! download_and_verify "$arch" "$SENTINEL_VERSION" "$new_binary" "$new_forward_binary"; then
        die "Failed to download and verify new binary"
    fi

    # Verify new binary is executable
    log_info "Verifying new binary..."

    if [[ ! -x "$new_binary" ]]; then
        log_error "Downloaded binary is not executable!"
        log_error "Binary path: $new_binary"
        log_error "Permissions: $(ls -l "$new_binary")"
        rm -f "$new_binary"
        die "Binary verification failed"
    fi

    if [[ -f "$new_forward_binary" ]]; then
        log_info "Verifying forward binary..."
        if [[ ! -x "$new_forward_binary" ]]; then
            log_error "Downloaded forward binary is not executable!"
            log_error "Binary path: $new_forward_binary"
            log_error "Permissions: $(ls -l "$new_forward_binary")"
            rm -f "$new_forward_binary"
            die "Forward binary verification failed"
        fi
        log_info "✓ Forward binary downloaded and verified successfully"
    fi

    # Try to get version from new binary (with timeout)
    log_info "Testing new binary..."
    local new_bin_version
    if new_bin_version=$(timeout 5 "$new_binary" --version 2>&1 | head -n1 || echo "version check failed"); then
        log_info "New binary version: $new_bin_version"
    else
        log_warn "Could not get version from binary (exit code: $?)"
        log_warn "This may indicate a dynamic linking issue"
        log_warn "Checking binary type..."
        file "$new_binary" || true
        ldd "$new_binary" 2>&1 | head -n 5 || true
    fi

    log_info "✓ New binary downloaded and verified successfully"

    # Create backup
    create_backup_binary "$BINARY_NAME"
    if [[ -f "${INSTALL_DIR}/${FORWARD_BINARY_NAME}" ]]; then
        create_backup_binary "$FORWARD_BINARY_NAME"
    fi

    # Install new binary
    install_new_binary_to "$new_binary" "${INSTALL_DIR}/${BINARY_NAME}"
    if [[ -f "$new_forward_binary" ]]; then
        install_new_binary_to "$new_forward_binary" "${INSTALL_DIR}/${FORWARD_BINARY_NAME}"
    fi

    # Restart service
    restart_service

    # Success
    local new_version
    new_version=$(get_installed_version)

    log_info ""
    log_info "=========================================="
    log_info "Update completed successfully!"
    log_info "=========================================="
    log_info "Previous version: $current_version"
    log_info "Current version: $new_version"
    log_info ""
    log_info "Service is running normally"
    log_info "View logs: sudo journalctl -u $SERVICE_NAME -f"
    log_info ""
    log_info "Rollback command (if needed):"
    log_info "  sudo cp ${INSTALL_DIR}/${BINARY_NAME}.prev ${INSTALL_DIR}/${BINARY_NAME}"
    log_info "  sudo systemctl restart $SERVICE_NAME"
    log_info ""
}

main "$@"
