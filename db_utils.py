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
        metadata = {}
        inspector = inspect(engine)
        
        for table_name in inspector.get_table_names():
            # 获取表注释
            try:
                table_comment = inspector.get_table_comment(table_name).get('text', '')
            except Exception:
                table_comment = ''
            
            # 获取主键信息
            primary_key = inspector.get_pk_constraint(table_name)['constrained_columns']
            
            # 获取列信息
            columns = {}
            for col in inspector.get_columns(table_name):
                # 获取列注释
                col_comment = col.get('comment', '')
                if not col_comment and 'comment' in col:
                    col_comment = col['comment']
                
                # 处理默认值
                default = col.get('default')
                if callable(default):
                    default = None
                
                columns[col['name']] = {
                    'type': str(col['type']),
                    'nullable': col['nullable'],
                    'default': default,
                    'comment': col_comment
                }
            
            # 存储表元数据
            metadata[table_name] = {
                'columns': columns,
                'primary_key': primary_key,
                'comment': table_comment
            }
        
        return metadata
    except exc.SQLAlchemyError as e:
        logger.error(f"获取表元数据失败: {e}")
        return {}

def compare_metadata(src_meta, tgt_meta):
    """比较源数据库和目标数据库的元数据差异，返回详细差异信息"""
    diff = {
        "tables": {
            "missing": [],  # 目标库缺失的表
            "extra": [],    # 目标库多余的表
            "changed": []   # 表结构有变化的表
        },
        "columns": {}       # 表内列的差异
    }
    
    # 比较表
    src_tables = set(src_meta.keys())
    tgt_tables = set(tgt_meta.keys())
    
    diff["tables"]["missing"] = list(src_tables - tgt_tables)
    diff["tables"]["extra"] = list(tgt_tables - src_tables)
    
    # 比较共有表的结构差异
    common_tables = src_tables & tgt_tables
    for table in common_tables:
        # 检查表注释是否相同
        src_comment = src_meta[table].get("comment", "")
        tgt_comment = tgt_meta[table].get("comment", "")
        comment_changed = src_comment != tgt_comment
        
        # 比较列
        src_cols = src_meta[table]["columns"]
        tgt_cols = tgt_meta[table]["columns"]
        
        col_diff = {
            "missing": [],  # 目标库缺失的列
            "extra": [],    # 目标库多余的列
            "changed": {}   # 列定义有变化的列
        }
        
        # 比较列差异
        src_col_names = set(src_cols.keys())
        tgt_col_names = set(tgt_cols.keys())
        
        col_diff["missing"] = list(src_col_names - tgt_col_names)
        col_diff["extra"] = list(tgt_col_names - src_col_names)
        
        # 比较共有列的差异
        common_cols = src_col_names & tgt_col_names
        for col in common_cols:
            src_def = src_cols[col]
            tgt_def = tgt_cols[col]
            
            changes = {}
            
            # 比较类型
            if src_def["type"] != tgt_def["type"]:
                changes["type"] = {
                    "src": src_def["type"],
                    "tgt": tgt_def["type"]
                }
            
            # 比较是否可为空
            if src_def["nullable"] != tgt_def["nullable"]:
                changes["nullable"] = {
                    "src": src_def["nullable"],
                    "tgt": tgt_def["nullable"]
                }
            
            # 比较默认值
            if src_def["default"] != tgt_def["default"]:
                changes["default"] = {
                    "src": src_def["default"],
                    "tgt": tgt_def["default"]
                }
            
            # 比较注释
            if src_def.get("comment") != tgt_def.get("comment"):
                changes["comment"] = {
                    "src": src_def.get("comment", ""),
                    "tgt": tgt_def.get("comment", "")
                }
            
            # 如果有任何变化，记录差异
            if changes:
                col_diff["changed"][col] = {
                    "src": src_def,
                    "tgt": tgt_def,
                    "changes": changes
                }
        
        # 如果有列差异或注释差异，标记表为已更改
        if col_diff["missing"] or col_diff["extra"] or col_diff["changed"] or comment_changed:
            diff["tables"]["changed"].append(table)
            diff["columns"][table] = col_diff
    
    return diff
