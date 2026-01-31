#!/bin/bash
# Sentinel RTP Camera - Management Script
# Quick access to common management commands

set -e

SERVICE_NAME_LEGACY="sentinel_rtp_cam"
SERVICE_NAME_FORWARD="sentinel_rtp_cam_forward"
CONFIG_FILE="/etc/sentinel_rtp_cam/env"
CLIPS_DIR="/var/lib/sentinel_rtp_cam/clips"
FORWARD_BIN="/usr/local/bin/${SERVICE_NAME_FORWARD}"
FORWARD_SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME_FORWARD}.service"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

show_usage() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  config       Edit configuration file"
    echo "  clips        List clips in storage directory"
    echo "  restart      Restart the service"
    echo "  stop         Stop the service"
    echo "  start        Start the service (defaults to forward mode)"
    echo "  status       Show service status"
    echo "  logs         Follow live logs"
    echo "  logs-recent  Show recent logs (last 50 lines)"
    echo "  clean        Delete all clips (with confirmation)"
    echo ""
    echo "Examples:"
    echo "  $0 config"
    echo "  $0 restart"
    echo "  $0 start forward"
    echo "  $0 start legacy"
    echo "  $0 logs"
    echo ""
    echo "Mode selection:"
    echo "  - If no mode is passed, the script defaults to forward mode."
    echo "  - Set AGENT_MODE=forward|legacy in $CONFIG_FILE to pin the mode."
    echo "  - If forward service is missing but ${FORWARD_BIN} exists, the script will create it."
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        echo -e "${YELLOW}This command requires sudo. Re-running with sudo...${NC}"
        exec sudo "$0" "$@"
    fi
}

systemd_service_exists() {
    local service="$1"
    systemctl cat "${service}.service" >/dev/null 2>&1
}

forward_binary_exists() {
    [[ -x "$FORWARD_BIN" ]]
}

ensure_forward_service() {
    if systemd_service_exists "$SERVICE_NAME_FORWARD"; then
        return 0
    fi
    if ! forward_binary_exists; then
        return 1
    fi

    echo -e "${YELLOW}Forward service not installed; creating ${SERVICE_NAME_FORWARD}.service${NC}"
    cat > "$FORWARD_SERVICE_FILE" <<EOF
[Unit]
Description=Sentinel RTP Camera Agent (forward)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=sentinel
Group=sentinel
WorkingDirectory=/var/lib/sentinel_rtp_cam

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/sentinel_rtp_cam
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictRealtime=true
RestrictNamespaces=true
LockPersonality=true

# Resource limits
LimitNOFILE=65536

# Environment
EnvironmentFile=/etc/sentinel_rtp_cam/env

# Execution
ExecStart=$FORWARD_BIN

# Restart policy
Restart=on-failure
RestartSec=10
StartLimitInterval=200
StartLimitBurst=5

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME_FORWARD

[Install]
WantedBy=multi-user.target
EOF
    chmod 644 "$FORWARD_SERVICE_FILE"
    systemctl daemon-reload
    return 0
}

resolve_mode() {
    local mode="${1:-}"
    if [[ -z "$mode" && -f "$CONFIG_FILE" ]]; then
        mode=$(grep -E '^AGENT_MODE=' "$CONFIG_FILE" | tail -n 1 | cut -d= -f2-)
        case "$mode" in
            forward|legacy) ;;
            *) mode="" ;;
        esac
        if [[ -z "$mode" ]]; then
            if grep -Eq '^SERVER_ADDR=' "$CONFIG_FILE"; then
                mode="forward"
            fi
        fi
    fi
    if [[ -z "$mode" ]]; then
        mode="forward"
    fi
    echo "$mode"
}

resolve_service() {
    local mode
    mode=$(resolve_mode "${1:-}")

    if [[ "$mode" == "forward" ]]; then
        ACTIVE_SERVICE="$SERVICE_NAME_FORWARD"
        ACTIVE_MODE="forward"
        if ! systemd_service_exists "$ACTIVE_SERVICE"; then
            ensure_forward_service || true
        fi
        if ! systemd_service_exists "$ACTIVE_SERVICE"; then
            echo -e "${YELLOW}Forward service not installed; falling back to legacy (${SERVICE_NAME_LEGACY})${NC}"
            ACTIVE_SERVICE="$SERVICE_NAME_LEGACY"
            ACTIVE_MODE="legacy"
        fi
    else
        ACTIVE_SERVICE="$SERVICE_NAME_LEGACY"
        ACTIVE_MODE="legacy"
        if ! systemd_service_exists "$ACTIVE_SERVICE" && systemd_service_exists "$SERVICE_NAME_FORWARD"; then
            echo -e "${YELLOW}Legacy service not installed; using forward (${SERVICE_NAME_FORWARD})${NC}"
            ACTIVE_SERVICE="$SERVICE_NAME_FORWARD"
            ACTIVE_MODE="forward"
        fi
    fi
}

cmd_config() {
    check_root
    nano "$CONFIG_FILE"
}

cmd_clips() {
    check_root
    echo -e "${GREEN}Clips in $CLIPS_DIR:${NC}"
    ls -lh "$CLIPS_DIR" || echo "No clips found or directory empty"
    echo ""
    echo -e "${BLUE}Disk usage:${NC}"
    du -sh "$CLIPS_DIR" 2>/dev/null || echo "N/A"
    df -h "$CLIPS_DIR"
}

cmd_restart() {
    check_root
    resolve_service "${2:-}"
    echo -e "${YELLOW}Restarting $ACTIVE_SERVICE ($ACTIVE_MODE)...${NC}"
    systemctl restart "$ACTIVE_SERVICE"
    sleep 2
    systemctl status "$ACTIVE_SERVICE" --no-pager
}

cmd_stop() {
    check_root
    resolve_service "${2:-}"
    echo -e "${YELLOW}Stopping $ACTIVE_SERVICE ($ACTIVE_MODE)...${NC}"
    systemctl stop "$ACTIVE_SERVICE"
    systemctl status "$ACTIVE_SERVICE" --no-pager
}

cmd_start() {
    check_root
    resolve_service "${2:-}"
    echo -e "${GREEN}Starting $ACTIVE_SERVICE ($ACTIVE_MODE)...${NC}"
    systemctl start "$ACTIVE_SERVICE"
    sleep 2
    systemctl status "$ACTIVE_SERVICE" --no-pager
}

cmd_status() {
    check_root
    resolve_service "${2:-}"
    systemctl status "$ACTIVE_SERVICE" --no-pager
}

cmd_logs() {
    check_root
    resolve_service "${2:-}"
    echo -e "${GREEN}Following logs for $ACTIVE_SERVICE ($ACTIVE_MODE) (Ctrl+C to exit)...${NC}"
    journalctl -u "$ACTIVE_SERVICE" -f
}

cmd_logs_recent() {
    check_root
    resolve_service "${2:-}"
    echo -e "${GREEN}Recent logs for $ACTIVE_SERVICE ($ACTIVE_MODE):${NC}"
    journalctl -u "$ACTIVE_SERVICE" -n 50 --no-pager
}

cmd_clean() {
    check_root
    echo -e "${RED}WARNING: This will delete ALL clips in $CLIPS_DIR${NC}"
    read -p "Are you sure? Type 'yes' to confirm: " confirm
    if [[ "$confirm" == "yes" ]]; then
        echo -e "${YELLOW}Deleting all clips...${NC}"
        rm -f "$CLIPS_DIR"/*.mp4 "$CLIPS_DIR"/*.mp4.part
        echo -e "${GREEN}Done. Clips deleted.${NC}"
    else
        echo "Cancelled."
    fi
}

# Main command dispatcher
case "${1:-}" in
    config)
        cmd_config
        ;;
    clips|ls)
        cmd_clips
        ;;
    restart)
        cmd_restart "$@"
        ;;
    stop)
        cmd_stop "$@"
        ;;
    start)
        cmd_start "$@"
        ;;
    status)
        cmd_status "$@"
        ;;
    logs)
        cmd_logs "$@"
        ;;
    logs-recent|recent)
        cmd_logs_recent "$@"
        ;;
    clean)
        cmd_clean
        ;;
    help|--help|-h|"")
        show_usage
        exit 0
        ;;
    *)
        echo -e "${RED}Error: Unknown command '$1'${NC}"
        echo ""
        show_usage
        exit 1
        ;;
esac
