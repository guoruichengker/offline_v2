#!/bin/bash

# SeaTunnel MySQL to Hive 数据同步脚本
# 作者：Gracie
# 日期：2025/09/16
# 版本: 4.3
# 描述: 动态获取MySQL表并同步到Hive ODS层，支持MySQL和Hive数据库参数传入

# 设置环境变量
SEATUNNEL_HOME="/opt/soft/apache-seatunnel-2.3.10"
CONFIG_DIR="$SEATUNNEL_HOME/user_config"
LOG_DIR="$SEATUNNEL_HOME/logs"
JOB_NAME="sync_mysql_to_hive"
MYSQL_HOST="192.168.200.32"
MYSQL_USER="root"
MYSQL_PASSWORD="root"
HIVE_HOST="192.168.200.32"
HIVE_PORT="10000"
HIVE_METASTORE_HOST="192.168.200.32"
HIVE_METASTORE_PORT="9083"
HDFS_BASE_PATH="hdfs://cdh01:8020/bigdata_warehouse"

# 默认数据库配置（可通过参数覆盖）
MYSQL_DATABASE="realtime_v1"
HIVE_DATABASE="bigdata_offline_v1_ws"

# 默认处理日期（前一天）
DO_DATE=$(date -d '-1 day' +%Y%m%d)

# 创建必要的目录
mkdir -p $CONFIG_DIR $LOG_DIR

# 颜色输出函数
color_echo() {
    local color=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case $color in
        "red")
            echo -e "\033[31m[${timestamp}] $message\033[0m" | tee -a $LOG_DIR/${JOB_NAME}_${DO_DATE}.log
            ;;
        "green")
            echo -e "\033[32m[${timestamp}] $message\033[0m" | tee -a $LOG_DIR/${JOB_NAME}_${DO_DATE}.log
            ;;
        "yellow")
            echo -e "\033[33m[${timestamp}] $message\033[0m" | tee -a $LOG_DIR/${JOB_NAME}_${DO_DATE}.log
            ;;
        "blue")
            echo -e "\033[34m[${timestamp}] $message\033[0m" | tee -a $LOG_DIR/${JOB_NAME}_${DO_DATE}.log
            ;;
        *)
            echo "[${timestamp}] $message" | tee -a $LOG_DIR/${JOB_NAME}_${DO_DATE}.log
            ;;
    esac
}

# 日志函数
log_info() {
    color_echo "blue" "INFO: $1"
}

log_success() {
    color_echo "green" "SUCCESS: $1"
}

log_warning() {
    color_echo "yellow" "WARNING: $1"
}

log_error() {
    color_echo "red" "ERROR: $1"
    exit 1
}

log_debug() {
    if [ "$DEBUG" = "true" ]; then
        color_echo "yellow" "DEBUG: $1"
    fi
}

# 执行Hive SQL命令
execute_hive_sql() {
    local sql=$1
    local description=$2
    
    log_info "执行Hive SQL: $description"
    log_debug "SQL: $sql"
    
    if command -v beeline &> /dev/null; then
        beeline -u "jdbc:hive2://${HIVE_HOST}:${HIVE_PORT}" -n root -e "$sql" >> $LOG_DIR/hive_sql_${DO_DATE}.log 2>&1
        local exit_code=$?
    else
        log_warning "beeline未安装，尝试使用hive命令"
        hive -e "$sql" >> $LOG_DIR/hive_sql_${DO_DATE}.log 2>&1
        local exit_code=$?
    fi
    
    if [ $exit_code -eq 0 ]; then
        log_success "Hive SQL执行成功: $description"
        return 0
    else
        log_error "Hive SQL执行失败: $description"
        return 1
    fi
}

# 创建Hive数据库
create_hive_database() {
    log_info "检查并创建Hive数据库: $HIVE_DATABASE"
    
    local db_location="${HDFS_BASE_PATH}/${HIVE_DATABASE}/"
    
    local create_db_sql="CREATE DATABASE IF NOT EXISTS ${HIVE_DATABASE} LOCATION '${db_location}' WITH DBPROPERTIES ('creator' = 'Gracie', 'created_date' = '$(date +%Y-%m-%d)');"
    
    if execute_hive_sql "$create_db_sql" "创建数据库 $HIVE_DATABASE"; then
        log_success "Hive数据库 $HIVE_DATABASE 创建/检查完成"
        return 0
    else
        log_warning "Hive数据库 $HIVE_DATABASE 创建失败，但继续执行"
        return 1
    fi
}

# 获取MySQL表结构信息
get_mysql_table_structure() {
    local mysql_table=$1
    local structure_file="$LOG_DIR/structure_${MYSQL_DATABASE}_${mysql_table}_${DO_DATE}.txt"
    
    log_info "获取MySQL表结构: $mysql_table"
    
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWORD -D$MYSQL_DATABASE -e "
        SELECT 
            COLUMN_NAME,
            COLUMN_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_NAME = '${mysql_table}' 
          AND TABLE_SCHEMA = '${MYSQL_DATABASE}'
        ORDER BY ORDINAL_POSITION;
    " --batch --silent --skip-column-names > "$structure_file" 2>/dev/null
    
    if [ $? -eq 0 ] && [ -s "$structure_file" ]; then
        log_success "成功获取表结构: $mysql_table"
        return 0
    else
        log_error "获取表结构失败: $mysql_table"
        return 1
    fi
}

# 映射MySQL数据类型到Hive数据类型
map_mysql_to_hive_type() {
    local mysql_type=$1
    
    case $(echo "$mysql_type" | tr '[:upper:]' '[:lower:]') in
        *int*)
            echo "STRING"
            ;;
        *tinyint*)
            echo "STRING"
            ;;
        *smallint*)
            echo "STRING"
            ;;
        *bigint*)
            echo "STRING"
            ;;
        *float*)
            echo "STRING"
            ;;
        *double*)
            echo "STRING"
            ;;
        *decimal*)
            if [[ "$mysql_type" =~ decimal\(([0-9]+),([0-9]+)\) ]]; then
                echo "STRING "
            else
                echo "STRING "
            fi
            ;;
        *char*|*text*|*enum*|*set*|*varchar*)
            echo "STRING"
            ;;
        *date*)
            echo "STRING"
            ;;
        *time*)
            echo "STRING"
            ;;
        *datetime*|*timestamp*)
            echo "STRING"
            ;;
        *boolean*|*bool*)
            echo "STRING"
            ;;
        *)
            echo "STRING"
            ;;
    esac
}

# 创建Hive外部表
create_hive_table() {
    local mysql_table=$1
    local hive_table="ods_${mysql_table}"
    local table_location="${HDFS_BASE_PATH}/${HIVE_DATABASE}/${hive_table}/"
    
    log_info "检查并创建Hive表: $hive_table"
    
    # 获取MySQL表结构
    if ! get_mysql_table_structure "$mysql_table"; then
        log_warning "无法获取表结构，跳过创建Hive表: $hive_table"
        return 1
    fi
    
    local structure_file="$LOG_DIR/structure_${MYSQL_DATABASE}_${mysql_table}_${DO_DATE}.txt"
    
    # 构建建表SQL
    local create_table_sql="USE ${HIVE_DATABASE}; CREATE EXTERNAL TABLE IF NOT EXISTS ${hive_table}("
    
    # 添加字段定义
    local first_field=true
    while IFS=$'\t' read -r column_name column_type; do
        if [ -z "$column_name" ]; then
            continue
        fi
        
        if [ "$first_field" = true ]; then
            first_field=false
        else
            create_table_sql+=", "
        fi
        
        local hive_type=$(map_mysql_to_hive_type "$column_type")
        create_table_sql+="${column_name} ${hive_type}"
    done < "$structure_file"
    
    # 添加分区字段和表属性
    create_table_sql+=") PARTITIONED BY (ds STRING) LOCATION '${table_location}' TBLPROPERTIES ('parquet.compress' = 'SNAPPY', 'external.table.purge' = 'true');"
    
    if execute_hive_sql "$create_table_sql" "创建表 $hive_table"; then
        log_success "Hive表 $hive_table 创建/检查完成"
        return 0
    else
        log_warning "Hive表 $hive_table 创建失败，但继续执行"
        return 1
    fi
}

# 检查MySQL连接并获取表列表
get_mysql_tables() {
    log_info "连接MySQL数据库获取表列表..."
    
    local tables_file="$LOG_DIR/mysql_tables_${MYSQL_DATABASE}_${DO_DATE}.txt"
    
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWORD -D$MYSQL_DATABASE -e "SHOW TABLES;" --batch --silent --skip-column-names > $tables_file 2>/dev/null
    
    if [ $? -eq 0 ] && [ -s "$tables_file" ]; then
        log_success "成功获取MySQL表列表，数据库: $MYSQL_DATABASE，共 $(wc -l < $tables_file) 张表"
        return 0
    else
        log_error "获取MySQL表列表失败，数据库: $MYSQL_DATABASE"
        return 1
    fi
}

# 检查表是否存在
check_table_exists() {
    local table_name=$1
    local tables_file="$LOG_DIR/mysql_tables_${MYSQL_DATABASE}_${DO_DATE}.txt"
    
    if [ -f "$tables_file" ] && grep -q "^${table_name}$" "$tables_file"; then
        return 0
    else
        return 1
    fi
}

# 检查SeaTunnel环境
check_env() {
    log_info "开始检查SeaTunnel环境..."
    
    if [ ! -d "$SEATUNNEL_HOME" ]; then
        log_error "SeaTunnel目录不存在: $SEATUNNEL_HOME"
    fi
    
    if [ ! -f "$SEATUNNEL_HOME/bin/seatunnel.sh" ]; then
        log_error "SeaTunnel启动脚本不存在: $SEATUNNEL_HOME/bin/seatunnel.sh"
    fi
    
    if [ ! -d "$CONFIG_DIR" ]; then
        mkdir -p $CONFIG_DIR
    fi
    
    if [ ! -d "$LOG_DIR" ]; then
        mkdir -p $LOG_DIR
    fi
    
    log_success "环境检查完成"
}

# 检查Hive连接
check_hive_connection() {
    log_info "检查Hive连接..."
    
    if command -v beeline &> /dev/null; then
        beeline -u "jdbc:hive2://${HIVE_HOST}:${HIVE_PORT}" -n hive -e "SHOW DATABASES;" >> $LOG_DIR/hive_check_${DO_DATE}.log 2>&1
        if [ $? -eq 0 ]; then
            log_success "Hive连接正常"
        else
            log_warning "Hive连接检查失败，但继续执行"
        fi
    else
        log_warning "beeline未安装，跳过Hive连接检查"
    fi
}

# 检查MySQL连接
check_mysql_connection() {
    log_info "检查MySQL连接..."
    
    if command -v mysql &> /dev/null; then
        mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWORD -e "SELECT 1;" >> $LOG_DIR/mysql_check_${DO_DATE}.log 2>&1
        if [ $? -eq 0 ]; then
            log_success "MySQL连接正常"
            return 0
        else
            log_error "MySQL连接检查失败"
            return 1
        fi
    else
        log_error "mysql客户端未安装，无法检查连接"
        return 1
    fi
}

# 生成配置文件
generate_config() {
    local mysql_table=$1
    local config_file="$CONFIG_DIR/sync_${MYSQL_DATABASE}_${mysql_table}_${DO_DATE}.conf"
    local hive_table="${HIVE_DATABASE}.ods_${mysql_table}"
    
    log_info "生成配置文件: $(basename $config_file)"
    
    # 动态生成查询语句，包含所有字段和日期分区字段
    local source_query=$(mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWORD -D$MYSQL_DATABASE -e "
        SELECT CONCAT('SELECT ', GROUP_CONCAT(' ', COLUMN_NAME), ', DATE_FORMAT(NOW(), ''%Y%m%d'') as ds FROM ', TABLE_SCHEMA, '.', TABLE_NAME)
        FROM information_schema.COLUMNS
        WHERE TABLE_NAME = '${mysql_table}' AND TABLE_SCHEMA = '${MYSQL_DATABASE}'
        GROUP BY TABLE_SCHEMA, TABLE_NAME;
    " --batch --silent --skip-column-names 2>/dev/null)
    
    if [ -z "$source_query" ]; then
        log_error "生成查询语句失败，表 ${mysql_table} 可能不存在或无法访问"
        return 1
    fi
    
    # 获取MySQL表的所有字段名
    local fields_file="$LOG_DIR/fields_${MYSQL_DATABASE}_${mysql_table}_${DO_DATE}.txt"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASSWORD -D$MYSQL_DATABASE -e "
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_NAME = '${mysql_table}' AND TABLE_SCHEMA = '${MYSQL_DATABASE}'
        ORDER BY ORDINAL_POSITION;
    " --batch --silent --skip-column-names > "$fields_file" 2>/dev/null
    
    if [ ! -s "$fields_file" ]; then
        log_error "获取字段列表失败，表 ${mysql_table} 可能不存在或无法访问"
        return 1
    fi
    
    # 构建fields数组字符串
    local fields_array="["
    local field_count=0
    local total_fields=$(wc -l < "$fields_file")
    
    while IFS= read -r field; do
        ((field_count++))
        if [ $field_count -eq $total_fields ]; then
            fields_array+="\"$field\""
        else
            fields_array+="\"$field\", "
        fi
    done < "$fields_file"
    
    fields_array+=", \"ds\"]"
    
    cat > $config_file << EOF
env {
    parallelism = 2
    job.mode = "BATCH"
}

source {
    Jdbc {
        url = "jdbc:mysql://${MYSQL_HOST}:3306/${MYSQL_DATABASE}?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "${MYSQL_USER}"
        password = "${MYSQL_PASSWORD}"
        query = "$source_query"
    }
}

transform {
    # 数据转换逻辑可以在这里添加
}

sink {
    Hive {
        table_name = "$hive_table"
        metastore_uri = "thrift://${HIVE_METASTORE_HOST}:${HIVE_METASTORE_PORT}"
        hive.hadoop.conf-path = "/etc/hadoop/conf"
        save_mode = "overwrite"
        partition_by = ["ds"]
        dynamic_partition = true
        parquet_compress = "SNAPPY"
        tbl_properties = {
            "external.table.purge" = "true"
        }
        fields = $fields_array
    }
}
EOF
    
    if [ $? -eq 0 ]; then
        log_success "配置文件生成成功: $(basename $config_file)"
        return 0
    else
        log_error "配置文件生成失败"
        return 1
    fi
}

# 执行SeaTunnel任务
execute_seatunnel() {
    local config_file=$1
    local mysql_table=$2
    local log_file="$LOG_DIR/${MYSQL_DATABASE}_${mysql_table}_${DO_DATE}.log"
    
    log_info "开始执行SeaTunnel任务: $mysql_table"
    
    local start_time=$(date +%s)
    
    cd $SEATUNNEL_HOME
    $SEATUNNEL_HOME/bin/seatunnel.sh -c $config_file -m local >> "$log_file" 2>&1
    
    local exit_code=$?
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    log_info "任务执行时间: ${duration}秒"
    
    if [ $exit_code -eq 0 ]; then
        log_success "SeaTunnel任务执行成功: $mysql_table"
        return 0
    else
        log_error "SeaTunnel任务执行失败: $mysql_table，退出码: $exit_code"
        log_info "请查看详细日志: $log_file"
        return 1
    fi
}

# 同步单张表
sync_table() {
    local mysql_table=$1
    
    log_info "══════════════════════════════════════════════════════════════"
    log_info "开始同步表: $mysql_table"
    log_info "MySQL数据库: $MYSQL_DATABASE"
    log_info "Hive数据库: $HIVE_DATABASE"
    log_info "处理日期: $DO_DATE"
    log_info "══════════════════════════════════════════════════════════════"
    
    if ! check_table_exists "$mysql_table"; then
        log_error "表 $mysql_table 在MySQL数据库 $MYSQL_DATABASE 中不存在"
        return 1
    fi
    
    create_hive_database
    create_hive_table "$mysql_table"
    
    local config_file="$CONFIG_DIR/sync_${MYSQL_DATABASE}_${mysql_table}_${DO_DATE}.conf"
    if ! generate_config "$mysql_table"; then
        return 1
    fi
    
    if execute_seatunnel "$config_file" "$mysql_table"; then
        log_info "══════════════════════════════════════════════════════════════"
        log_success "表 $mysql_table 同步完成"
        log_info "══════════════════════════════════════════════════════════════"
        return 0
    else
        return 1
    fi
}

# 同步所有表
sync_all_tables() {
    local tables_file="$LOG_DIR/mysql_tables_${MYSQL_DATABASE}_${DO_DATE}.txt"
    local success_count=0
    local fail_count=0
    
    if [ ! -f "$tables_file" ] || [ ! -s "$tables_file" ]; then
        log_error "表列表文件不存在或为空，请先获取表列表"
        return 1
    fi
    
    log_info "开始批量同步所有表..."
    log_info "数据库: $MYSQL_DATABASE"
    log_info "共发现 $(wc -l < $tables_file) 张表需要同步"
    
    create_hive_database
    
    while IFS= read -r table; do
        if [ -n "$table" ]; then
            if sync_table "$table"; then
                ((success_count++))
            else
                ((fail_count++))
            fi
            sleep 1
        fi
    done < "$tables_file"
    
    log_info "══════════════════════════════════════════════════════════════"
    log_info "批量同步完成统计:"
    log_info "数据库: $MYSQL_DATABASE"
    log_info "成功: $success_count 张表"
    log_info "失败: $fail_count 张表"
    log_info "总计: $((success_count + fail_count)) 张表"
    log_info "══════════════════════════════════════════════════════════════"
    
    if [ $fail_count -eq 0 ]; then
        return 0
    else
        return 1
    fi
}

# 显示可用的表列表
show_available_tables() {
    local tables_file="$LOG_DIR/mysql_tables_${MYSQL_DATABASE}_${DO_DATE}.txt"
    
    if [ ! -f "$tables_file" ] || [ ! -s "$tables_file" ]; then
        log_error "表列表文件不存在或为空，请先获取表列表"
        return 1
    fi
    
    echo "可用的MySQL表列表 (数据库: $MYSQL_DATABASE):"
    echo "══════════════════════════════════════════════════════════════"
    cat $tables_file | awk '{printf "%-25s", $1; if (NR % 3 == 0) print ""} END {if (NR % 3 != 0) print ""}'
    echo "══════════════════════════════════════════════════════════════"
    echo "共 $(wc -l < $tables_file) 张表"
}

# 显示使用帮助
show_usage() {
    echo "用法: $0 [选项] [MySQL表名]"
    echo "选项:"
    echo "  -d, --date DATE          指定处理日期 (格式: YYYYMMDD)"
    echo "  -m, --mysql-db DB        指定MySQL数据库名"
    echo "  -h, --hive-db DB         指定Hive数据库名"
    echo "  -a, --all                同步所有表"
    echo "  -l, --list               显示可用的表列表"
    echo "  -v, --verbose            详细模式"
    echo "  --help                   显示帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 -m realtime_v1 -h bigdata_offline_v1_ws activity_rule"
    echo "  $0 --mysql-db test_db --hive-db test_hive --all"
    echo "  $0 --mysql-db production --list"
    echo "  $0 -m sales_db user_info --verbose"
    echo "  $0 --help"
}

# 主函数
main() {
    local mysql_table=""
    local sync_all=false
    local list_tables=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--date)
                DO_DATE="$2"
                shift 2
                ;;
            -m|--mysql-db)
                MYSQL_DATABASE="$2"
                shift 2
                ;;
            -h|--hive-db)
                HIVE_DATABASE="$2"
                shift 2
                ;;
            -a|--all)
                sync_all=true
                shift
                ;;
            -l|--list)
                list_tables=true
                shift
                ;;
            -v|--verbose)
                DEBUG="true"
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            *)
                mysql_table="$1"
                shift
                ;;
        esac
    done
    
    log_info "══════════════════════════════════════════════════════════════"
    log_info "SeaTunnel MySQL to Hive 数据同步任务启动"
    log_info "开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
    log_info "MySQL主机: $MYSQL_HOST"
    log_info "MySQL数据库: $MYSQL_DATABASE"
    log_info "Hive数据库: $HIVE_DATABASE"
    log_info "处理日期: $DO_DATE"
    log_info "══════════════════════════════════════════════════════════════"
    
    check_env
    if ! check_mysql_connection; then
        log_error "MySQL连接失败，无法继续执行"
    fi
    check_hive_connection
    
    if ! get_mysql_tables; then
        log_error "获取表列表失败，无法继续执行"
    fi
    
    if [ "$list_tables" = true ]; then
        show_available_tables
        exit 0
    fi
    
    if [ "$sync_all" = true ]; then
        sync_all_tables
    elif [ -n "$mysql_table" ]; then
        sync_table "$mysql_table"
    else
        show_usage
        exit 1
    fi
    
    log_info "══════════════════════════════════════════════════════════════"
    log_success "数据同步任务完成"
    log_info "结束时间: $(date '+%Y-%m-%d %H:%M:%S')"
    log_info "配置文件保存在: $CONFIG_DIR/"
    log_info "详细日志请查看: $LOG_DIR/"
    log_info "══════════════════════════════════════════════════════════════"
}

# 执行主函数
main "$@"