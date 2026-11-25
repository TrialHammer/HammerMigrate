# 实现结构

一个 JAR 承载平台入口、JDBC/NBT 桥和相同的 Python 引擎。迁移命令只维护持久队列；正式读写发生在游戏进程启动前。分布式节点通过共享状态目录协调，数据库按实例身份与库名分配给一个节点处理。

下表中的 `engine/` 位于源码目录 `src/main/resources/engine/`，打包后随 JAR 分发。

| 目录或模块 | 职责 |
| --- | --- |
| `src/main/java/ink/ziip/hammer/migrate` | Paper/Bungee 入口、引擎提取和 Java 桥 |
| `engine/discovery.py`、`configuration.py` | 当前文件、插件和数据库连接发现 |
| `engine/scanning.py` | 并行物理扫描、进度和 NBT 检查点 |
| `engine/identity.py`、`relational.py` | 身份语义和明确支持的账号合并 |
| `engine/files.py`、`nbt.py`、`database.py` | 格式适配、行校验和可逆操作 |
| `engine/worker.py`、`distributed.py` | 任务执行、启动屏障、取消和恢复 |
| `bin/deploy.py` | 主机部署、发布包、一致性检查及部署回退 |
| `tests` | 独立文件、数据库、进程和部署回归 |

`manifest.json` 是持久扫描报告，不是固定文件清单。每次执行重新发现范围；检查点只复用摘要与规则匹配的结果。数据库在写入事务内重新检查身份候选范围，避免在扫描后新增的玩家引用被遗漏。

正式写入先记录操作意图并持久化，再执行文件补丁或数据库事务。数据库同时校验完整行镜像、唯一约束、真实外键和受支持插件的逻辑引用。回退使用原始行和逆向补丁，发现后续修改时停止。

未知插件字段、未支持的合并和范围之外的数据不能由通用扫描器保证迁移；它们需要适配器、字段规则或明确的范围配置。每次迁移应以实际 manifest 和部署范围为依据。
