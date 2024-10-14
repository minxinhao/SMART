from typing import Optional

# sed 是一个强大的文本处理工具，常用于替换文件中的特定文本。这里的 sed 命令通过正则表达式查找并替换 C++ 配置文件中的某些定义。
# sed -i 's/old_text/new_text/g' filename
# -i：表示直接修改文件，而不是输出到终端。
# •	s/old_text/new_text/：这是最常用的替换命令，表示将匹配的 old_text 替换为 new_text。
# •	g：表示全局替换，即替换文件中所有匹配的部分，而不只是第一处匹配。


# 修改配置文件中key长度的函数
def sed_key_len(config_path: str, key_size: int):
    old_key_code   = "^constexpr uint32_t keyLen = .*" # 匹配旧的key长度代码，使用正则表达式匹配
    new_key_code   = f"constexpr uint32_t keyLen = {key_size};" # 根据key_size生成新的key长度代码
    return f"sed -i 's/{old_key_code}/{new_key_code}/g' {config_path}" # 使用sed命令替换配置文件中的key长度

# 修改配置文件中value长度的函数
def sed_val_len(config_path: str, value_size: int):
    old_val_code   = "^constexpr uint32_t simulatedValLen =.*"
    new_val_code   = f"constexpr uint32_t simulatedValLen = {value_size};"
    return f"sed -i 's/{old_val_code}/{new_val_code}/g' {config_path}"


def sed_cache_size(config_path: str, cache_size: int):
    old_cache_code = "^constexpr int kIndexCacheSize = .*"
    new_cache_node = f"constexpr int kIndexCacheSize = {cache_size};"
    return f"sed -i 's/{old_cache_code}/{new_cache_node}/g' {config_path}"


def sed_MN_num(config_path: str, MN_num: int):
    old_MN_code = "^#define MEMORY_NODE_NUM .*"
    new_MN_node = f"#define MEMORY_NODE_NUM {MN_num}"
    return f"sed -i 's/{old_MN_code}/{new_MN_node}/g' {config_path}"


def sed_span_size(config_path: str, span_size: int):  # only for Sherman
    old_span_code = "^constexpr int spanSize = .*"
    new_span_node = f"constexpr int spanSize = {span_size};"
    return f"sed -i 's/{old_span_code}/{new_span_node}/g' {config_path}"


def generate_sed_cmd(config_path: str, is_Btree: bool, key_size: int, value_size: int, cache_size: int, MN_num: int, span_size: Optional[int] = None):
    cmd = f"{sed_key_len(config_path, key_size)} && {sed_val_len(config_path, value_size)} && {sed_cache_size(config_path, cache_size)} && {sed_MN_num(config_path, MN_num)}"
    if is_Btree:  # change span size for Sherman
        assert(span_size is not None)
        cmd += f"&& {sed_span_size(config_path, span_size)}"
    return cmd
