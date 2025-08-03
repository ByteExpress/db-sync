import os
import json
import logging
from datetime import datetime
import re
from flask import Flask, render_template, request, jsonify, send_file
from db_utils import get_engine, get_table_metadata, compare_metadata

app = Flask(__name__)
CONFIG_FILE = "configs/connections.json"

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 加载配置
def load_config():
    try:
        with open(CONFIG_FILE) as f:
            return json.load(f)["connections"]
    except FileNotFoundError:
        logger.error(f"配置文件 {CONFIG_FILE} 不存在")
        return []
    except json.JSONDecodeError:
        logger.error(f"配置文件 {CONFIG_FILE} 格式错误")
        return []

def should_exclude_table(table_name,exclude_tables):
        """检查表是否应该被排除"""
        for pattern in exclude_tables:
            # 支持通配符 * 匹配
            if pattern.endswith('*'):
                prefix = pattern[:-1]
                if table_name.startswith(prefix):
                    return True
            # 完全匹配
            elif pattern == table_name:
                return True
        return False

# 添加当前时间到模板上下文
@app.context_processor
def inject_now():
    return {'now': datetime.now()}

# 读取脚本内容
@app.route('/read_script', methods=['POST'])
def read_script():
    try:
        data = request.json
        path = data['path']
        
        if not os.path.exists(path):
            return jsonify({"error": "文件不存在"}), 404
            
        with open(path, 'r') as f:
            content = f.read()
            
        return jsonify({"script": content})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 执行脚本
@app.route('/execute_script', methods=['POST'])
def execute_script():
    try:
        data = request.json
        conn_id = data["conn_id"]
        script = data["script"]
        
        # 获取目标数据库配置
        configs = load_config()
        conn_config = next((c for c in configs if c["id"] == conn_id), None)
        if not conn_config:
            return jsonify({"success": False, "message": "无效的连接ID"}), 400
        
        # 连接到目标数据库
        engine = get_engine(conn_config["target"])
        
        # 执行脚本
        with engine.connect() as conn:
            # 分割SQL语句（简单实现，实际应使用更可靠的方法）
            statements = [stmt for stmt in script.split(';') if stmt.strip()]
            for stmt in statements:
                conn.execute(f"{stmt};")
        
        return jsonify({"success": True, "message": "脚本执行成功"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

# 主页 - 显示连接配置
@app.route('/')
def index():
    configs = load_config()
    return render_template('index.html', connections=configs)

# 比较页面
@app.route('/compare/<conn_id>')
def compare(conn_id):
    configs = load_config()
    conn_config = next((c for c in configs if c["id"] == conn_id), None)
    
    if not conn_config:
        return "无效的连接ID", 404
    
    try:
        # 获取元数据
        src_engine = get_engine(conn_config["source"])
        tgt_engine = get_engine(conn_config["target"])
        
        src_meta = get_table_metadata(src_engine)
        tgt_meta = get_table_metadata(tgt_engine)
        
        if not src_meta or not tgt_meta:
            return "无法获取数据库元数据", 500
            
        diff = compare_metadata(src_meta, tgt_meta)
        
        # 计算统计信息
        stats = {
            "table_diff": len(diff["tables"]["missing"]) + len(diff["tables"]["changed"]),
            "column_diff": 0,
            "missing_tables": len(diff["tables"]["missing"]),
            "extra_tables": len(diff["tables"]["extra"])
        }
        
        # 计算列差异总数
        for table, col_diff in diff["columns"].items():
            stats["column_diff"] += len(col_diff["missing"]) + len(col_diff["changed"])

        exclude_tables = conn_config.get('exclude_tables', [])
        # 准备表格数据
        src_tables = []
        for table, meta in src_meta.items():
            # 检查表是否在排除列表中
            if should_exclude_table(table,exclude_tables):
                continue

            table_status = ""
            col_statuses = []
            
            if table in diff["tables"]["missing"]:
                table_status = "missing"
            elif table in diff["tables"]["changed"]:
                table_status = "changed"
            
            for col, col_def in meta["columns"].items():
                col_status = "normal"
                if table in diff["columns"]:
                    if col in diff["columns"][table]["missing"]:
                        col_status = "missing"
                    elif col in diff["columns"][table]["changed"]:
                        col_status = "changed"
                col_statuses.append({"name": col, "type": str(col_def["type"]), "status": col_status})
            
            src_tables.append({
                "name": table,
                "status": table_status,
                "columns": col_statuses
            })
        
        tgt_tables = []
        for table, meta in tgt_meta.items():
            # 检查表是否在排除列表中
            if should_exclude_table(table,exclude_tables):
                continue
            
            table_status = ""
            if table in diff["tables"]["extra"]:
                table_status = "extra"
            
            col_statuses = []
            for col, col_def in meta["columns"].items():
                col_status = "normal"
                if table in diff["columns"] and col in diff["columns"][table]["extra"]:
                    col_status = "extra"
                col_statuses.append({"name": col, "type": str(col_def["type"]), "status": col_status})
            
            tgt_tables.append({
                "name": table,
                "status": table_status,
                "columns": col_statuses
            })
        
        return render_template('compare.html', 
                              conn_id=conn_id,
                              src_db_name=conn_config["source"]["database"],
                              tgt_db_name=conn_config["target"]["database"],
                              stats=stats,
                              src_tables=src_tables,
                              tgt_tables=tgt_tables,
                              diff=diff)
    except Exception as e:
        logger.error(f"数据库比较失败: {e}")
        return f"数据库连接或比较失败: {str(e)}", 500

# 保存新连接
@app.route('/save_connection', methods=['POST'])
def save_connection():
    try:
        conn_data = request.json
        
        # 加载现有配置
        configs = load_config()
        
        # 检查ID是否已存在
        if any(c['id'] == conn_data['id'] for c in configs):
            return jsonify({
                "success": False,
                "message": f"连接ID '{conn_data['id']}' 已存在"
            })
        
        # 添加新连接
        configs.append(conn_data)
        
        # 保存到文件
        with open(CONFIG_FILE, 'w') as f:
            json.dump({"connections": configs}, f, indent=2)
        
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"保存连接失败: {e}")
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500
    
@app.route('/update_exclude_tables', methods=['POST'])
def update_exclude_tables():
    try:
        data = request.json
        conn_id = data['conn_id']
        exclude_tables = data['exclude_tables']
        
        # 加载现有配置
        configs = load_config()
        
        # 找到对应的连接
        conn_index = next((i for i, c in enumerate(configs) if c['id'] == conn_id), None)
        
        if conn_index is None:
            return jsonify({
                "success": False,
                "message": f"找不到连接ID '{conn_id}'"
            }), 404
        
        # 更新排除表配置
        configs[conn_index]['exclude_tables'] = exclude_tables
        
        # 保存到文件
        with open(CONFIG_FILE, 'w') as f:
            json.dump({"connections": configs}, f, indent=2)
        
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"更新排除表失败: {e}")
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500
    
# 更新现有连接
@app.route('/update_connection', methods=['PUT'])
def update_connection():
    try:
        update_data = request.json
        original_id = update_data.get('original_id')
        new_id = update_data.get('id')
        
        if not original_id:
            return jsonify({
                "success": False,
                "message": "缺少原始连接ID参数"
            }), 400
        
        # 加载现有配置
        configs = load_config()
        
        # 查找原始连接
        original_index = None
        for i, conn in enumerate(configs):
            if conn['id'] == original_id:
                original_index = i
                break
        
        if original_index is None:
            return jsonify({
                "success": False,
                "message": f"找不到原始连接ID '{original_id}'"
            }), 404
        
        # 检查新ID是否已存在（且不是当前连接）
        if new_id and new_id != original_id:
            if any(c['id'] == new_id for c in configs):
                return jsonify({
                    "success": False,
                    "message": f"新连接ID '{new_id}' 已存在"
                })
        
        # 更新连接数据
        updated_conn = configs[original_index]
        
        # 更新ID（如果已更改）
        if new_id and new_id != original_id:
            updated_conn['id'] = new_id
        
        # 更新源数据库信息
        source = update_data.get('source')
        if source:
            updated_conn['source'] = {
                **updated_conn['source'],
                **source
            }
        
        # 更新目标数据库信息
        target = update_data.get('target')
        if target:
            updated_conn['target'] = {
                **updated_conn['target'],
                **target
            }
        
        # 保存到文件
        with open(CONFIG_FILE, 'w') as f:
            json.dump({"connections": configs}, f, indent=2)
        
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"更新连接失败: {e}")
        return jsonify({
            "success": False,
            "message": str(e)
        }), 500

def generate_sync_script(conn_id, src_meta, tgt_meta, diff, selected_tables, selected_columns):
    """生成精确的数据库同步脚本"""
    script = f"-- 数据库同步脚本: {conn_id}\n"
    script += f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    script += f"-- 同步模式: 结构同步\n\n"
    
    # 生成表注释
    script += f"-- 共选择 {len(selected_tables)} 个表进行同步\n"
    
    # 生成表级操作
    for table in selected_tables:
        # 跳过未选中的表
        if table not in selected_tables:
            continue
            
        # 获取表的列信息
        src_cols = src_meta[table]["columns"]
        tgt_cols = tgt_meta[table]["columns"] if table in tgt_meta else {}
        
        # 处理缺失的表（目标库不存在）
        if table in diff["tables"]["missing"]:
            script += f"\n-- 创建缺失的表: {table}\n"
            script += f"CREATE TABLE {table} (\n"
            
            # 添加列定义
            col_definitions = []
            for col, col_def in src_cols.items():
                # 只添加选中的列
                if col in selected_columns.get(table, []):
                    col_def_str = f"    {col} {col_def['type']}"
                    if not col_def["nullable"]:
                        col_def_str += " NOT NULL"
                    if col_def.get("default") is not None:
                        default_val = col_def["default"]
                        # 处理默认值
                        if isinstance(default_val, str) and not re.match(r"^'.*'$", default_val):
                            default_val = f"'{default_val}'"
                        col_def_str += f" DEFAULT {default_val}"
                    col_definitions.append(col_def_str)
            
            script += ",\n".join(col_definitions)
            
            # 添加主键
            if src_meta[table].get("primary_key"):
                pk_cols = ", ".join(src_meta[table]["primary_key"])
                script += f",\n    PRIMARY KEY ({pk_cols})"
            
            script += "\n);\n"
        
        # 处理已有表的结构变更
        elif table in diff["tables"]["changed"]:
            table_diff = diff["columns"][table]
            script += f"\n-- 同步表结构变更: {table}\n"
            
            # 添加缺失的列
            for col in table_diff["missing"]:
                # 只添加选中的列
                if col in selected_columns.get(table, []):
                    col_def = src_cols[col]
                    col_def_str = f"ALTER TABLE {table} ADD COLUMN {col} {col_def['type']}"
                    if not col_def["nullable"]:
                        col_def_str += " NOT NULL"
                    if col_def.get("default") is not None:
                        default_val = col_def["default"]
                        if isinstance(default_val, str) and not re.match(r"^'.*'$", default_val):
                            default_val = f"'{default_val}'"
                        col_def_str += f" DEFAULT {default_val}"
                    script += col_def_str + ";\n"
            
            # 修改列定义
            for col, changes in table_diff["changed"].items():
                # 只处理选中的列
                if col in selected_columns.get(table, []):
                    src_def = changes["src"]
                    col_def_str = f"ALTER TABLE {table} MODIFY COLUMN {col} {src_def['type']}"
                    if not src_def["nullable"]:
                        col_def_str += " NOT NULL"
                    if src_def.get("default") is not None:
                        default_val = src_def["default"]
                        if isinstance(default_val, str) and not re.match(r"^'.*'$", default_val):
                            default_val = f"'{default_val}'"
                        col_def_str += f" DEFAULT {default_val}"
                    script += col_def_str + ";\n"
            
            # 删除多余的列（可选，默认不删除）
            # for col in table_diff["extra"]:
            #     script += f"-- ALTER TABLE {table} DROP COLUMN {col};  -- 谨慎: 删除多余列\n"
    
    # 处理多余的表（源库不存在，目标库存在） - 可选删除
    extra_tables = [t for t in diff["tables"]["extra"] if t in selected_tables]
    if extra_tables:
        script += "\n-- 目标库多余的表（源库不存在）\n"
        for table in extra_tables:
            script += f"-- DROP TABLE {table};  -- 谨慎: 删除多余表\n"
    
    script += "\n-- 同步完成 --\n"
    return script

# 生成同步脚本
@app.route('/generate', methods=['POST'])
def generate_script():
    try:
        data = request.json
        conn_id = data["conn_id"]
        selected_tables = data["tables"]
        selected_columns = data.get("columns", {})
        output_path = data.get("output_path", "")
        
        # 获取数据库连接配置
        configs = load_config()
        conn_config = next((c for c in configs if c["id"] == conn_id), None)
        if not conn_config:
            return jsonify({"status": "error", "message": "无效的连接ID"}), 400
        
        # 获取元数据
        src_engine = get_engine(conn_config["source"])
        tgt_engine = get_engine(conn_config["target"])
        
        src_meta = get_table_metadata(src_engine)
        tgt_meta = get_table_metadata(tgt_engine)
        
        # 计算差异
        diff = compare_metadata(src_meta, tgt_meta)
        
        # 生成精确的同步脚本
        script = generate_sync_script(
            conn_id, 
            src_meta, 
            tgt_meta, 
            diff, 
            selected_tables, 
            selected_columns
        )
        
        # 保存到文件
        if output_path:
            try:
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                with open(output_path, 'w') as f:
                    f.write(script)
                return jsonify({"status": "saved", "path": output_path})
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500
        
        return jsonify({"script": script})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    os.makedirs("configs", exist_ok=True)
    app.run(debug=True)