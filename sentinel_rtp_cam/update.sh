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
readonly INSTALL_DIR="/usr/local/bin"
readonly CONFIG_DIR="/etc/${BINARY_NAME}"
readonly STATE_DIR="/var/lib/${BINARY_NAME}"
readonly SERVICE_NAME="${BINARY_NAME}"

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
    local binary="${INSTALL_DIR}/${BINARY_NAME}"
    if [[ ! -x "$binary" ]]; then
        echo "none"
        return
    fi
    
    # Try to get version from binary (assuming it supports --version)
    local version
    version=$("$binary" --version 2>/dev/null | head -n1 | awk '{print $NF}') || echo "unknown"
    echo "$version"
}

download_and_verify() {
    local arch="$1"
    local version="$2"
    local output_path="$3"
    
    # Handle "latest" by fetching latest release tag from GitHub
    if [[ "$version" == "latest" ]]; then
        log_info "Fetching latest release version from GitHub..."
        version=$(curl -fsSL --max-time 30 "https://api.github.com/repos/${SENTINEL_REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"v([^"]+)".*/\1/')
        if [[ -z "$version" ]]; then
            log_error "Failed to fetch latest version from GitHub"
            log_error "Check: https://github.com/${SENTINEL_REPO}/releases"
            return 1
        fi
        log_info "Latest version: $version"
    fi
    
    local tarball_name="${BINARY_NAME}-${version}-${arch}.tar.gz"
    local download_url="${SENTINEL_BASE_URL}/v${version}/${tarball_name}"
    local checksum_url="${SENTINEL_BASE_URL}/v${version}/${tarball_name}.sha256"
    local temp_dir
    temp_dir=$(mktemp -d)
    local tarball_path="${temp_dir}/${tarball_name}"
    
    log_info "Downloading ${tarball_name} from GitHub releases..."
    log_info "URL: $download_url"
    
    if ! curl -fL --progress-bar --max-time 300 -o "$tarball_path" "$download_url"; then
        log_error "Download failed. Check if release exists:"
        log_error "https://github.com/${SENTINEL_REPO}/releases/tag/v${version}"
        rm -rf "$temp_dir"
        return 1
    fi
    
    # Verify checksum (always available from GitHub releases)
    log_info "Verifying checksum..."
    local expected_sha
    expected_sha=$(curl -fsSL --max-time 30 "$checksum_url" | awk '{print $1}')
    if [[ -z "$expected_sha" ]]; then
        log_warn "Could not fetch checksum, skipping verification"
        log_warn "Checksum URL: $checksum_url"
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
    
    # Move to output path
    mv "${temp_dir}/${BINARY_NAME}" "$output_path"
    chmod 755 "$output_path"
    
    rm -rf "$temp_dir"
    return 0
}

create_backup() {
    local current="${INSTALL_DIR}/${BINARY_NAME}"
    local backup="${INSTALL_DIR}/${BINARY_NAME}.prev"
    
    if [[ ! -f "$current" ]]; then
        log_warn "No current binary to back up"
        return
    fi
    
    if [[ $DRY_RUN -eq 1 ]]; then
        log_dry "Would back up: $current -> $backup"
        return
    fi
    
    log_info "Creating backup: $backup"
    cp -f "$current" "$backup"
}

install_new_binary() {
    local new_binary="$1"
    local target="${INSTALL_DIR}/${BINARY_NAME}"
    
    if [[ $DRY_RUN -eq 1 ]]; then
        log_dry "Would install: $new_binary -> $target"
        return
    fi
    
    log_info "Installing new binary..."
    mv -f "$new_binary" "$target"
    chmod 755 "$target"
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
}

verify_service() {
    if [[ $DRY_RUN -eq 1 ]]; then
        log_dry "Would verify service health"
        return 0
    fi
    
    log_info "Verifying service started successfully..."
    log_info "Waiting for service to stabilize..."
    sleep 5
    
    if systemctl is-active --quiet "$SERVICE_NAME"; then
        log_info "Service is running"
        
        # Check recent logs for errors
        local error_count
        error_count=$(journalctl -u "$SERVICE_NAME" --since "30 seconds ago" -p err --no-pager | wc -l)
        
        if [[ $error_count -gt 0 ]]; then
            log_warn "Found $error_count error(s) in recent logs"
            log_warn "Recent logs:"
            journalctl -u "$SERVICE_NAME" --since "30 seconds ago" --no-pager | tail -n 10
            return 1
        fi
        
        return 0
    else
        log_error "Service failed to start"
        log_error "Recent logs:"
        journalctl -u "$SERVICE_NAME" --since "1 minute ago" --no-pager | tail -n 20
        return 1
    fi
}

rollback() {
    local backup="${INSTALL_DIR}/${BINARY_NAME}.prev"
    local current="${INSTALL_DIR}/${BINARY_NAME}"
    
    if [[ ! -f "$backup" ]]; then
        log_error "No backup available for rollback"
        return 1
    fi
    
    log_warn "Rolling back to previous version..."
    cp -f "$backup" "$current"
    
    log_info "Restarting service with previous version..."
    systemctl restart "$SERVICE_NAME"
    
    sleep 3
    if systemctl is-active --quiet "$SERVICE_NAME"; then
        log_info "Rollback successful - service is running"
        return 0
    else
        log_error "Rollback failed - service still not running"
        return 1
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
    
    # Check if service is installed
    if [[ ! -f "${INSTALL_DIR}/${BINARY_NAME}" ]]; then
        die "Service not installed. Run install.sh first."
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
    
    # Download new binary to temporary location
    local new_binary="${INSTALL_DIR}/${BINARY_NAME}.new"
    
    if [[ $DRY_RUN -eq 1 ]]; then
        log_dry "Would download version $SENTINEL_VERSION for $arch"
        log_dry "Would install to: $new_binary"
        log_info "Dry-run completed successfully"
        exit 0
    fi
    
    if ! download_and_verify "$arch" "$SENTINEL_VERSION" "$new_binary"; then
        die "Failed to download and verify new binary"
    fi
    
    # Verify new binary is executable
    if [[ ! -x "$new_binary" ]]; then
        rm -f "$new_binary"
        die "Downloaded binary is not executable"
    fi
    
    log_info "New binary downloaded successfully"
    
    # Create backup
    create_backup
    
    # Install new binary
    install_new_binary "$new_binary"
    
    # Restart service
    restart_service
    
    # Verify service health
    if ! verify_service; then
        log_error "Service health check failed after update"
        log_warn "Attempting rollback..."
        
        if rollback; then
            die "Update failed but rollback successful"
        else
            die "Update failed and rollback failed - manual intervention required"
        fi
    fi
    
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
