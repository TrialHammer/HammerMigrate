# 迁移操作

## 命令与工作流程

以下命令中的 `OldName` 和 `NewName` 是示例用户名。命令支持自动补全子命令、选项和值。

1. 执行只读扫描。分布式模式会将扫描加入队列，关闭并启动所有参与服务器后开始扫描。集中模式的扫描需要网络处于关闭状态。

   ```text
   /hmigrate dry-run OldName NewName --premium off
   /hmigrate status
   /hmigrate progress
   ```

2. 查看 manifest、覆盖范围、冲突、待审字段和跳过项。确认结果符合预期后，将正式迁移加入队列。

   ```text
   /hmigrate queue OldName NewName --premium off
   ```

3. 关闭并启动所有参与服务器。启动前重新扫描、校验并执行队列。查看 `status` 和迁移日志，确认任务状态。

`scan` 与 `dry-run` 使用相同的只读预检。`--fresh` 强制重新解析 NBT 和 region 检查点。数据库每次都会重新查询。`queue`、`scan` 和 `dry-run` 支持 `--premium on|off` 和 `--merge`，未指定正版选项时使用 `premiumDefault`。

```text
/hmigrate progress [任务ID]
/hmigrate status
/hmigrate cancel <任务ID>
/hmigrate retry <任务ID>
/hmigrate rollback <任务ID>
```

`cancel` 适用于未执行或已完全恢复的失败任务。`retry` 为已完全恢复的失败任务创建新任务。`rollback` 将已完成或需要恢复的任务加入回退队列，回退在服务器关闭后的启动阶段进行。连续迁移同一账号时，需要先回退后续任务。

命令行提供相同的队列和扫描入口，另外提供集中模式的 `discover` 和 `run`。

```bash
java -jar /path/to/HammerMigrate.jar --config /path/to/network.json status
java -jar /path/to/HammerMigrate.jar --config /path/to/network.json progress
java -jar /path/to/HammerMigrate.jar --config /path/to/network.json discover
```

分布式模式使用 `dry-run` 和参与者启动流程完成发现。

## 分布式启动控制台

服务器启动前，启动器在控制台等待任务执行。输入以下命令时无需 `/hmigrate` 前缀。容器必须保持标准输入通道打开，才能向启动器发送命令。

```text
help
status
cancel <任务ID>
retry <任务ID>
rollback <任务ID>
```

`status` 显示持久队列中的任务及错误。`cancel` 可结束未执行的任务，也可请求停止正在发现、扫描或执行的任务。各参与者需要确认停止，涉及写入时会先依据日志恢复，再将任务标记为已取消。节点缺席时，启动器等待其重新加入以完成协调。恢复或回退进行中时，需要等待该步骤结束。

`retry` 和 `rollback` 使用与插件命令相同的队列规则。恢复失败时，检查任务日志并处理原因，再使用 `rollback` 安排恢复。没有可用的控制台输入时，可以通过命令行查看队列。全部参与者启动器停止后，中断的分布式 dry-run 可使用以下命令取消。

```bash
java -jar /path/to/HammerMigrate.jar --config /path/to/network.json \
  cancel <任务ID> --force
```

## 目标账号与合并

默认情况下，目标用户名或 UUID 已有数据会产生冲突并阻止迁移。明确指定 `--merge` 才允许尝试合并。支持的合并会保留双方独有数据，发生字段冲突时以旧账号数据为主。PlayerPoints 余额累加，超出插件整数范围时阻止迁移；用户名缓存、AuthMe 账号和 PlayerParticles 设置以旧账号为主。PlayerParticles 只修改玩家所有者引用，保留组 UUID、粒子 UUID 及子表引用。同名粒子组以旧组为主，目标组改名为 `recovery-target-*` 后保留。

合并依赖具体存储格式和字段适配。尚未支持的冲突仍会列入 manifest 并阻止执行，不能仅凭指定 `--merge` 判断所有数据均可合并。迁移前应逐项检查目标账号冲突和计划中的修改。

## 正版登录

`--premium on` 会查询 Mojang，并要求目标用户名大小写与官方名字一致。原账号已绑定 `premiumUUID` 时，新名字必须属于相同 Mojang UUID。支持的 AuthMe/AuthMeBungee 正版验证配置会更新绑定和代理缓存，并清除旧登录会话。游戏 UUID 使用新用户名计算出的离线 UUID。直连 Paper 要求 `online-mode=false`、`settings.enablePremium=true`、`Hooks.bungeecord=false` 并安装 PacketEvents；无需代理缓存。使用 AuthMeBungee 的网络还需要代理离线 UUID 兼容配置与正版缓存。

`--premium off` 会清除支持的账号记录中的 `premiumUUID`、移除旧名字的代理正版缓存并清除登录会话。原密码散列随账号保留。使用正版登录需要相应的验证插件和离线 UUID 兼容配置，缺失条件会出现在扫描结果中。

## 扫描范围和报告

扫描支持玩家数据路径、JSON、YAML、CSV、Properties、TOML、NBT、Anvil region 和外部区块，以及 SQLite、H2、HyperSQL、MySQL 和 MariaDB。自动发现读取支持的插件存储配置，检查库、表、字段和身份引用。记录中的文件名可能属于其他玩家，扫描会检查其中对迁移账号的引用。

每次任务执行和 `retry` 都重新发现当前插件、文件与数据库，历史 manifest 用于审计。通过 JAR 描述符识别插件：已卸载插件的残留文件仍参与物理扫描，残留配置不会自动连接已停用的数据库；显式配置的 `databaseEndpoints` 仍需可用。相关前缀库的发现独立保留，可通过 `ignoredDatabaseEndpoints` 明确排除。

扫描开始后立即写出未完成的 manifest 和进度，完成后生成覆盖报告和具体计划。扫描期间范围发生变化时，最多按 `scanMaxPasses` 重扫（默认 3，范围 1–10），并重新分配发生变化的共享数据库。主要状态文件位于状态目录。

| 路径 | 内容 |
| --- | --- |
| `queue.json` | 持久任务队列及状态 |
| `scans/<任务ID>/manifest.json` | dry-run 结果与覆盖信息 |
| `runs/<任务ID>/manifest.json` | 正式迁移的扫描结果 |
| `runs/<任务ID>/plan.json.gz` | 文件补丁、数据库行和路径移动计划 |
| `runs/<任务ID>/journal.jsonl` | 操作意图、完成和回退记录 |
| `scan-cache.sqlite` | NBT、region 和外部区块的扫描检查点 |
| `participants/<节点>/` | 分布式节点的进度、清单、计划及检查点 |
| `distributed/` | 节点注册、锁和协同执行状态 |

扫描结束时会重新检查目录、文件属性和数据库发现范围。未处理的身份命中、冲突和常规扫描错误会阻止 `ready=true`。检查 `coverage.complete`、`ready`、`unresolved`、`errors`、冲突以及所有排除记录，并核对配置的服务器、数据库和外部权威数据源是否覆盖实际范围。

扫描器将 region 扫描异常和 SQLite 打开异常记录在 `ignoredScanErrors`，并继续扫描。SQLite 打开后的规划、约束和引用校验失败会阻止迁移。这些记录可能包含未被完整检查的数据，`ready=true` 本身不能证明这些区域或数据库已完成检查。查看报告时须逐项处理这些记录。

检查点按文件摘要、外部区块摘要、玩家身份、字段规则、物理根目录、引擎和 JAR 版本校验。插件清单、节点清单、数据库分工和扫描线程数变化时，保留仍有效的 NBT 检查点。相同条件下再次扫描可复用有效结果，仍会读取文件计算摘要。`--fresh` 强制重新解析。中断时未完成的 manifest 保持未就绪，尚未完成的文件会在后续扫描中重新处理。

`scanThreads` 控制每个节点并行扫描物理文件的线程数，范围为 1–32，默认值为 1。数据库扫描按顺序执行。提高线程数会增加并发磁盘读取和内存占用，实际性能取决于工作负载。配置在扫描启动时读取。

## 自动发现与配置

连接配置自动发现支持 YAML，以及插件目录中名为 config、configuration、storage、database、settings 的 JSON、TOML、简单 Properties 文件。Properties 支持普通键值和点分节名；复杂转义或多行语法需要显式连接配置。连接清单只保留配置路径与字段引用，不复制明文密码。PlayerPoints、PlayerParticles 的自定义表前缀由连接配置映射到相同的语义适配器。

`discoverRelatedSchemas` 默认启用，使用已发现的连接查找符合默认规则的相关库。可通过 `schemaPatterns` 明确库名范围。`databaseEndpoints` 可指定 MySQL 连接，凭据支持 `env:变量名`。清单保存凭据引用，避免公开密码。

`ignoredDatabaseEndpoints` 可按 `host`、`port` 和 `database` 精确排除已确认不使用的数据库，并使用 `reason` 记录原因。自动发现的配置文件仍会参与文件扫描。`ignorePaths` 使用网络根目录下的相对路径，目录项覆盖其子树，排除项会列入报告。分布式模式中，路径前缀使用 `servers` 中的节点名。

`embeddedDatabases` 可为嵌入式数据库指定连接参数。`fieldRules.location` 是与 manifest 中字段位置完整匹配的正则表达式，角色可设为 `uuid`、`name`、`identity`、`preserve` 或 `unknown`。无法判断的身份引用需要审阅或增加适配。`maxFileMiB` 控制单文件大小上限，默认为 256 MiB。

身份字段按语义处理。玩家所有者引用可迁移，世界 UUID、实体 UUID、显示名、签名皮肤来源和加密酒数据等保留原有语义。各插件的具体覆盖取决于实际数据结构和发现结果。

## 恢复与回退

正式迁移前检查网络协调锁、世界锁和监听端口。文件修改前检查内容摘要，数据库操作检查行镜像和表结构。数据库写入在同一事务中重新生成身份候选操作并与扫描计划比较；扫描后新增的身份引用、目标归属变化或新约束会拒绝旧计划。执行过程先记录意图，再记录完成状态。发生失败时按逆序恢复已开始的操作，未完成的恢复会保留在队列中供后续处理。

回退依赖迁移记录保存的逆向补丁、数据库行和路径映射。文件、数据库行或表结构在迁移后变化时，回退会停止并报告冲突。dry-run 失败，以及尚未写入的预检失败（`preflightBlocked=true`），会保留报告并允许服务器启动。处理报告中的问题后，执行 `retry <任务ID>` 重新发现和扫描。已开始写入的失败及恢复状态仍会阻止启动，需要检查任务日志并完成恢复。

## 判断是否完整

| `completion` | 含义 | 可执行性 |
| --- | --- | --- |
| `complete` | 已发现范围完整扫描、无未处理身份引用或冲突 | 可执行 |
| `with-exclusions` | 按明确的忽略策略跳过部分异常，不能证明这些数据已迁移 | 仅非严格策略可执行 |
| `blocked` | 存在范围变化、约束错误、未适配字段或严格覆盖要求未满足 | 保留报告并延期 |

`coverage.accounted` 表示每个发现项都已扫描、失败或明确记录为忽略；`coverage.complete` 还要求没有未扫描的异常项。部署示例使用 `requireCompleteCoverage: true`。未设置该选项时默认允许报告中的可忽略异常；需要严格覆盖时，请显式设为 `true`。严格覆盖预检未通过会保留报告并延期迁移；尚未写入的任务不阻止服务器启动。
