#!/opt/bin/bash
declare -x PATH="/opt/bin:/opt/sbin:/opt/bin:/opt/sbin:/opt/bin:/opt/sbin:/opt/bin:/opt/sbin:/bin:/usr/bin:/sbin:/usr/sbin:/rom/scripts:/home/xxhhlk:/mmc/sbin:/mmc/bin:/mmc/usr/sbin:/mmc/usr/bin:/opt/sbin:/opt/bin:/opt/usr/sbin:/opt/usr/bin"
renice -n -10 -p $$ > /dev/null 2>&1
INTERFACE="wan0"
INTERVAL=10

# 检查网络接口是否存在
if [ ! -d "/sys/class/net/$INTERFACE" ]; then
    exit 1
fi

# 检查统计文件是否可读
if [ ! -r "/sys/class/net/$INTERFACE/statistics/tx_bytes" ]; then
    exit 1
fi

# 初始化历史数据数组
declare -a TX_HISTORY

# 获取初始值
TX_CURRENT=$(cat /sys/class/net/$INTERFACE/statistics/tx_bytes 2>/dev/null)
if [ -z "$TX_CURRENT" ]; then
    exit 1
fi

# 填充历史数组初始值
for ((i=0; i<INTERVAL; i++)); do
    TX_HISTORY[i]=$TX_CURRENT
done

INDEX=0

sleep 1

while true; do
    # 记录循环开始时间（毫秒级）
    START_TIME=$(date +%s%N)
    
    # 获取当前值
    TX_CURRENT=$(cat /sys/class/net/$INTERFACE/statistics/tx_bytes 2>/dev/null)
    
    # 检查读取是否成功
    if [ -z "$TX_CURRENT" ]; then
        sleep 1
        continue
    fi
    
    # 计算与INTERVAL秒前的差值
    TX_DIFF=$((TX_CURRENT - TX_HISTORY[INDEX]))
    
    # 处理计数器重置的情况（差值为负）
    if [ $TX_DIFF -lt 0 ]; then
        TX_DIFF=$TX_CURRENT
    fi
    
    # 计算平均速度 KB/s (保留两位小数)
    TX_KB=$(awk "BEGIN {printf \"%.2f\", $TX_DIFF / 1024 / $INTERVAL}")
    
    # 只输出数值（不换行）
    echo "$TX_KB"
    
    # 更新历史数组
    TX_HISTORY[INDEX]=$TX_CURRENT
    
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
