#!/opt/bin/bash
declare -x PATH="/opt/bin:/opt/sbin:/opt/bin:/opt/sbin:/opt/bin:/opt/sbin:/opt/bin:/opt/sbin:/bin:/usr/bin:/sbin:/usr/sbin:/rom/scripts:/home/xxhhlk:/mmc/sbin:/mmc/bin:/mmc/usr/sbin:/mmc/usr/bin:/opt/sbin:/opt/bin:/opt/usr/sbin:/opt/usr/bin"
renice -n -10 -p $$ > /dev/null 2>&1
INTERFACE="ppp0"
WG_IFACE="wg0"
INTERVAL=10
PACKET_OVERHEAD=8
# BT 隧道 QUIC 流在 ppp0(IPv6) 上的目标端口（hysteria2 client -> 云端 hy2-server）
TUN_DPORT=45767
IP6T=/usr/sbin/ip6tables

# 检查网络接口是否存在
if [ ! -d "/sys/class/net/$INTERFACE" ]; then
    exit 1
fi

# 检查统计文件是否可读
if [ ! -r "/sys/class/net/$INTERFACE/statistics/tx_bytes" ]; then
    exit 1
fi
if [ ! -r "/sys/class/net/$INTERFACE/statistics/tx_packets" ]; then
    exit 1
fi

# 幂等确保隧道流量计数规则存在（mangle 计数器，无动作纯统计）。
# 固件上 iptables -C 不可靠，改用 -S grep 精确检查。
# 注意：-S 输出 "--dport N"（空格），-L 输出 "dpt:N"（冒号），两种都查。
if [ -x "$IP6T" ]; then
    if ! "$IP6T" -t mangle -S POSTROUTING 2>/dev/null | grep -Eq -- "dport ${TUN_DPORT}|dpt:${TUN_DPORT}"; then
        "$IP6T" -t mangle -I POSTROUTING 1 -o "$INTERFACE" -p udp --dport "$TUN_DPORT" 2>/dev/null
    fi
fi

# 读取隧道 QUIC 流累计字节（mangle 计数器；规则缺失或 ip6tables 不可用时记 0）
read_tun_bytes() {
    if [ -x "$IP6T" ]; then
        "$IP6T" -t mangle -L POSTROUTING -v -n -x 2>/dev/null | grep "dpt:${TUN_DPORT}" | awk '{print $2}' | head -n 1
    else
        echo 0
    fi
}

# 读取 wg0 内层净载荷累计字节（接口不存在时记 0）
read_wg_bytes() {
    if [ -r "/sys/class/net/$WG_IFACE/statistics/tx_bytes" ]; then
        cat "/sys/class/net/$WG_IFACE/statistics/tx_bytes" 2>/dev/null
    else
        echo 0
    fi
}

# 初始化历史数据数组
declare -a TX_HISTORY
declare -a PKT_HISTORY
declare -a TUN_HISTORY
declare -a WG_HISTORY

# 获取初始值
TX_CURRENT=$(cat /sys/class/net/$INTERFACE/statistics/tx_bytes 2>/dev/null)
PKT_CURRENT=$(cat /sys/class/net/$INTERFACE/statistics/tx_packets 2>/dev/null)
TUN_CURRENT=$(read_tun_bytes)
WG_CURRENT=$(read_wg_bytes)
if [ -z "$TX_CURRENT" ] || [ -z "$PKT_CURRENT" ] || [ -z "$TUN_CURRENT" ] || [ -z "$WG_CURRENT" ]; then
    exit 1
fi

# 填充历史数组初始值
for ((i=0; i<INTERVAL; i++)); do
    TX_HISTORY[i]=$TX_CURRENT
    PKT_HISTORY[i]=$PKT_CURRENT
    TUN_HISTORY[i]=$TUN_CURRENT
    WG_HISTORY[i]=$WG_CURRENT
done

INDEX=0

sleep 1

while true; do
    # 记录循环开始时间（毫秒级）
    START_TIME=$(date +%s%N)

    # 获取当前值
    TX_CURRENT=$(cat /sys/class/net/$INTERFACE/statistics/tx_bytes 2>/dev/null)
    PKT_CURRENT=$(cat /sys/class/net/$INTERFACE/statistics/tx_packets 2>/dev/null)
    TUN_CURRENT=$(read_tun_bytes)
    WG_CURRENT=$(read_wg_bytes)

    # 检查读取是否成功
    if [ -z "$TX_CURRENT" ] || [ -z "$PKT_CURRENT" ] || [ -z "$TUN_CURRENT" ] || [ -z "$WG_CURRENT" ]; then
        sleep 1
        continue
    fi

    # 计算字节差值和包数差值
    TX_DIFF=$((TX_CURRENT - TX_HISTORY[INDEX]))
    PKT_DIFF=$((PKT_CURRENT - PKT_HISTORY[INDEX]))
    TUN_DIFF=$((TUN_CURRENT - TUN_HISTORY[INDEX]))
    WG_DIFF=$((WG_CURRENT - WG_HISTORY[INDEX]))

    # 处理计数器重置的情况（差值为负）
    if [ $TX_DIFF -lt 0 ]; then
        TX_DIFF=$TX_CURRENT
    fi
    if [ $PKT_DIFF -lt 0 ]; then
        PKT_DIFF=$PKT_CURRENT
    fi
    # 隧道/wg0 计数器重置（规则重插、接口重建）时按 0 处理，宁少勿多
    if [ $TUN_DIFF -lt 0 ]; then
        TUN_DIFF=0
    fi
    if [ $WG_DIFF -lt 0 ]; then
        WG_DIFF=0
    fi

    # WAN 总速（补 PPPoE 每包 8 字节开销，与原口径一致）
    WAN_ALIGNED_DIFF=$((TX_DIFF + PKT_DIFF * PACKET_OVERHEAD))

    # 计算平均速度 KB/s (保留两位小数)
    WAN_KB=$(awk "BEGIN {printf \"%.2f\", $WAN_ALIGNED_DIFF / 1024 / $INTERVAL}")
    TUN_KB=$(awk "BEGIN {printf \"%.2f\", $TUN_DIFF / 1024 / $INTERVAL}")
    WG_KB=$(awk "BEGIN {printf \"%.2f\", $WG_DIFF / 1024 / $INTERVAL}")

    # 输出三列：WAN 总速 隧道QUIC流速度 wg0内层速度
    echo "$WAN_KB $TUN_KB $WG_KB"

    # 更新历史数组
    TX_HISTORY[INDEX]=$TX_CURRENT
    PKT_HISTORY[INDEX]=$PKT_CURRENT
    TUN_HISTORY[INDEX]=$TUN_CURRENT
    WG_HISTORY[INDEX]=$WG_CURRENT

    # 更新索引（循环）
    INDEX=$(( (INDEX + 1) % INTERVAL ))

    # 计算已用时间（毫秒），补偿式sleep
    END_TIME=$(date +%s%N)
    ELAPSED_MS=$(( (END_TIME - START_TIME) / 1000000 ))
    SLEEP_MS=$(( 1000 - ELAPSED_MS ))
    if [ $SLEEP_MS -gt 0 ]; then
        sleep "$(awk "BEGIN {printf \"%.3f\", $SLEEP_MS / 1000}")"
    fi
done
