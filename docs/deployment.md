# 部署与升级

在能够访问所有服务器目录和共享目录的主机上执行部署工具。容器内不需要安装 Python；运行时由部署工具放入共享目录。主机需要 Python 3.11+，源码部署还需要 Java 21+ 和 Maven。MySQL 8 的非 TLS RSA 密码交换还需要在 Python 运行时提供 `cryptography`，连接失败会记录并阻止迁移。预构建发布包可直接部署，无需 Maven。

## 一键部署

复制 [部署配置示例](../examples/deployment.json) 到主机上的独立位置，填写共享目录及各服数据目录。相对路径以部署配置所在目录为基准。服务器目录需要预先存在。配置不包含数据库密码，迁移时从插件配置或环境变量读取。

```bash
./hammermigrate deploy --profile /path/deployment.json --dry-run
./hammermigrate deploy --profile /path/deployment.json
./hammermigrate check --profile /path/deployment.json
```

源码部署会先运行构建和回归检查，再安装同一份 JAR、共享启动器和节点脚本。已有的有效 Python 运行时会复用；新部署会下载并校验固定版本的 glibc、musl 运行时，也可通过 `runtimeSource` 使用本地主机上已有的完整运行时。

指定 `--jar /path/HammerMigrate.jar` 可以部署已经验证的 JAR。发布包内包含 JAR，直接运行相同的部署命令即可。使用 `./hammermigrate package` 构建可分发的压缩包；`--jar` 可复用已验证构建。发布包同时提供文件摘要和归档 SHA-256。

## 目录约定

```text
共享目录（容器挂载为 /network）/
├── HammerMigrate/
│   ├── network.json              共享迁移配置
│   ├── HammerMigrate.jar         命令行与一致性检查
│   ├── bin/start-server.sh       共享启动器
│   ├── runtime/                  glibc / musl Python 运行时
│   ├── engines/                  按 JAR 摘要提取的引擎
│   ├── deployments/              部署记录及去重的回退内容
│   └── state/                    新部署的持久队列和迁移记录
└── migrate/HammerMigrate/        旧部署的状态目录，升级时保留

各服数据目录/
├── start-hammermigrate.sh        仅设置节点名和本服路径
└── plugins/HammerMigrate.jar     独立安装的插件
```

已有状态目录保持原位，部署不会搬动队列或回退记录。新部署使用配置中的 `stateDirectory: "state"`，相对于共享配置目录解析。`deployments/blobs` 根据内容摘要去重，同一版本的 JAR 无需为每个节点重复备份。

## Docker 启动命令

每个容器只挂载自己的数据目录和共享目录。将原启动命令作为节点脚本参数传入，例如：

```bash
bash /data/start-hammermigrate.sh java -jar server.jar --nogui
```

部署工具不修改 Docker、面板或容器配置，也不重启服务器。首次安装需要将容器启动命令接入上面的脚本，后续升级继续使用同一入口。控制台需要保持标准输入打开，才能在迁移启动阶段发送 `status`、`cancel` 等命令。

节点脚本只保留节点名、本服目录和共享路径；启动逻辑在共享目录维护一份。游戏进程使用 `umask 022`，常规新文件为 `0644`、新目录为 `0755`，便于独立 Web 服务读取地图。迁移引擎使用 `077` 保护日志与行镜像。`umask` 不会修复已有文件的权限，也不会覆盖插件显式设置的权限或 ACL。

## 更新失败和部署回退

更新采用逐文件原子替换，并保存每个文件的前后摘要。写入中途失败会自动恢复已经更新的文件。若主机进程被强制终止，按部署 ID 继续回退：

```bash
./hammermigrate rollback --profile /path/deployment.json --deployment <部署ID>
```

部署回退与玩家迁移回退分别记录。部署回退不修改玩家数据、数据库或迁移队列；文件被人工修改后会拒绝覆盖。迁移正在执行或需要恢复时，部署工具拒绝更新。变更参与者拓扑前需先停止旧启动器并完成原任务恢复。

## 运行模式与手动安装

集中模式适合一个启动进程能够访问整个网络数据的部署。分布式模式适合每个容器只访问自己的服务器数据，并通过共享目录协调的部署。所有参与者必须使用同一份配置、相同的 JAR 和引擎版本，并能访问配置的共享状态目录。

使用一键部署时，无需重复以下手动安装步骤。以下内容用于理解共享配置，或接入自定义启动流程。

### 集中模式

将服务器目录放在一个网络根目录下，使用命令生成配置。`init` 发现根目录下包含 `server.properties` 或 `BungeeCord.jar` 的直属子目录。

```bash
java -jar /path/to/HammerMigrate.jar \
  --config /path/to/network.json init --root /path/to/server-root
```

检查生成的 `networkRoot` 和 `servers`。所有服务 JVM 通过以下参数读取同一配置。

```text
-Dhammermigrate.config=/path/to/network.json
```

默认 `startup` 为 `external`。关闭所有参与服务器及其他数据库写入进程后，在启动任何服务前运行队列。

```bash
java -jar /path/to/HammerMigrate.jar --config /path/to/network.json run
```

也可以使用 `bin/start-after-migration.sh`，通过 `HAMMERMIGRATE_JAR` 指定 JAR。脚本在队列执行成功后启动传入的命令。

```bash
HAMMERMIGRATE_JAR=/path/to/HammerMigrate.jar \
  bash bin/start-after-migration.sh /path/to/network.json /path/to/start-network.sh
```

`startup: "embedded"` 可由插件加载阶段处理队列，适用于能够确保其余服务已关闭且插件先于数据加载的启动流程。启动检查失败时 JVM 会以非零状态终止。

### 分布式容器模式

每个容器挂载自己的服务器目录，并挂载同一个可写的共享目录。参与者可以全部为直连 Paper，迁移协调不依赖代理。手动安装时，将相同的 JAR 放到每个服务器的 `plugins/HammerMigrate.jar`。所有容器使用同一份配置，`servers` 中的名称须与各节点的 `HAMMERMIGRATE_SERVER` 一致。插件优先读取 `-Dhammermigrate.config`；未指定时自动使用已挂载的 `/network/HammerMigrate/network.json`，否则使用本地插件配置。迁移任务仍需通过启动器在游戏进程启动前协调执行。

下面的路径和名称是示例，可按环境替换。

```json
{
  "deployment": "distributed",
  "networkRoot": "..",
  "servers": ["backend-a", "backend-b", "backend-c"],
  "startup": "external",
  "python": "runtime/python",
  "premiumDefault": false,
  "scanThreads": 4,
  "scanMaxPasses": 3,
  "databaseEndpoints": [],
  "fieldRules": [],
  "ignorePaths": []
}
```

`networkRoot` 相对于配置文件所在目录解析，默认状态目录是 `<networkRoot>/migrate/HammerMigrate`。分布式启动模板 `bin/start-server.sh` 读取 `/network/HammerMigrate/network.json`，以脚本所在目录作为本服数据目录。将模板放在各服务器数据目录，设置对应的 `HAMMERMIGRATE_SERVER`，并把原启动命令作为参数传入。共享目录须挂载到模板使用的 `/network` 路径。

```bash
HAMMERMIGRATE_SERVER=backend-a \
  bash /path/to/server-data/start-server.sh java -jar server.jar --nogui
```

脚本向游戏 JVM 注入配置路径，并在启动游戏后转发 SIGTERM 和 SIGINT。作为参数传入的包装脚本应使用 `exec` 启动游戏 JVM。共享目录和服务器目录需要对各容器的运行用户可读写。

容器没有 Python 时，可以在具备 Python 3 和网络访问的主机上安装共享运行时。安装器提供 Linux x86_64 的 glibc 和 musl 构建，校验下载文件的 SHA-256，并复制 CA 证书。安装目录必须不存在。

```bash
python3 bin/install-python-runtime.py --directory /path/to/shared/HammerMigrate/runtime
```

配置 `python` 可以是 PATH 中的命令名、绝对路径，或相对于配置文件目录的路径。共享运行时的 `runtime/python` 会根据容器 libc 选择对应解释器。

参与者启动后先会合，检查服务已关闭并发现本地数据。共享 MySQL 数据库按数据库服务器身份和库名分配给参与者。所有参与者完成扫描与检查后才进入正式迁移。节点缺席时其余节点等待，应用中断时依据日志执行恢复。移除服务器时更新共享配置的 `servers`，新增服务器需要安装 JAR 并接入同一启动器。已开始写入的任务需要先用原参与者完成恢复，再改变拓扑。
