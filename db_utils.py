from sqlalchemy import create_engine, MetaData, inspect, exc
from sqlalchemy.engine.url import URL
import logging

logger = logging.getLogger(__name__)

def get_engine(conn_config):
    """创建数据库引擎，支持多种数据库类型"""
    dialect = conn_config['dialect']
    
    try:
        if dialect == 'sqlite':
            return create_engine(f"sqlite:///{conn_config['database']}")
        
        # 处理不同数据库的驱动问题
        driver_mapping = {
            'mysql': 'mysqlconnector' if 'mysqlconnector' in conn_config.get('driver', '') else 'pymysql',
            'postgresql': 'psycopg2',
            'mssql': 'pyodbc',
            'oracle': 'cx_oracle'
        }
        
        # 使用映射获取正确的驱动
        dialect_with_driver = f"{dialect}+{driver_mapping.get(dialect, '')}"
        
        url = URL.create(
            drivername=dialect_with_driver,
            username=conn_config['user'],
            password=conn_config['password'],
            host=conn_config['host'],
            port=conn_config['port'],
            database=conn_config['database']
        )
        return create_engine(url, pool_pre_ping=True)
    except ImportError as e:
        logger.error(f"缺少数据库驱动: {e}")
        raise
    except exc.SQLAlchemyError as e:
        logger.error(f"创建数据库引擎失败: {e}")
        raise

def get_table_metadata(engine):
    """获取数据库表的元数据"""
    try:
        inspector = inspect(engine)
        metadata = {}
        
        for table_name in inspector.get_table_names():
            columns = {}
            for col in inspector.get_columns(table_name):
                columns[col['name']] = {
                    "type": str(col['type']),
                    "nullable": col['nullable'],
                    "default": col.get('default', None)
                }
            
            # 获取主键信息
            try:
                pk_info = inspector.get_pk_constraint(table_name)
                metadata[table_name] = {
                    "columns": columns,
                    "primary_key": pk_info.get('constrained_columns', [])
                }
            except exc.NoSuchTableError:
                logger.warning(f"表 {table_name} 不存在或无法访问")
                continue
                
        return metadata
    except exc.SQLAlchemyError as e:
        logger.error(f"获取表元数据失败: {e}")
        return {}

def compare_metadata111(src_meta, tgt_meta):
    """比较源数据库和目标数据库的元数据差异"""
    diff = {"tables": {}, "columns": {}}
    
    # 表级差异
    for table in src_meta:
        if table not in tgt_meta:
            diff["tables"][table] = "MISSING"
            continue
            
        # 列级差异
        col_diff = {}
        for col, col_meta in src_meta[table]["columns"].items():
            if col not in tgt_meta[table]["columns"]:
                col_diff[col] = "MISSING"
            elif col_meta != tgt_meta[table]["columns"][col]:
                col_diff[col] = "DIFFERENT"
        
        if col_diff:
            diff["columns"][table] = col_diff
    
    # 检查目标数据库多余的表
    for table in tgt_meta:
        if table not in src_meta:
            diff["tables"][table] = "EXTRA"
    
    return diff

def compare_metadata(src_meta, tgt_meta):
    """比较源数据库和目标数据库的元数据差异，返回详细差异信息"""
    diff = {
        "tables": {
            "missing": [],    # 目标库缺失的表
            "extra": [],      # 目标库多余的表
            "changed": []     # 表结构有变化的表
        },
        "columns": {}
    }
    
    # 检测表级差异
    src_tables = set(src_meta.keys())
    tgt_tables = set(tgt_meta.keys())
    
    diff["tables"]["missing"] = list(src_tables - tgt_tables)  # 目标库缺失的表
    diff["tables"]["extra"] = list(tgt_tables - src_tables)    # 目标库多余的表
    
    # 检测列级差异
    for table in src_tables & tgt_tables:  # 两边都存在的表
        col_diff = {
            "missing": [],      # 目标表缺失的列
            "extra": [],        # 目标表多余的列
            "changed": {}       # 列定义有变化的列
        }
        
        src_cols = src_meta[table]["columns"]
        tgt_cols = tgt_meta[table]["columns"]
        
        for col_name, src_def in src_cols.items():
            if col_name not in tgt_cols:
                col_diff["missing"].append(col_name)
            else:
                tgt_def = tgt_cols[col_name]
                # 检查列定义是否相同
                if (src_def["type"] != tgt_def["type"] or 
                    src_def["nullable"] != tgt_def["nullable"] or
                    src_def.get("default") != tgt_def.get("default")):
                    col_diff["changed"][col_name] = {
                        "src": src_def,
                        "tgt": tgt_def
                    }
        
        # 检查目标表多余的列
        for col_name in tgt_cols.keys():
            if col_name not in src_cols:
                col_diff["extra"].append(col_name)
        
        # 如果表有列差异，则标记为changed表
        if col_diff["missing"] or col_diff["extra"] or col_diff["changed"]:
            diff["tables"]["changed"].append(table)
            diff["columns"][table] = col_diff
    
    return diff