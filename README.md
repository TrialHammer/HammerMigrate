# HammerMigrate

迁移离线玩家的用户名及离线 UUID，支持 Paper、可选 BungeeCord 和多容器启动前协调。包名为 `ink.ziip.hammer.migrate`。

扫描自动发现当前文件与数据库，生成持久 manifest 和可回退计划。正式迁移在服务器启动前执行；命令负责排队、查看、重试、取消和回退。

文档中的用户名、节点名和路径均为示例，请按自己的部署环境替换。部署配置、数据库凭据和迁移报告应独立保存，不要提交到源码仓库。

## 一键部署

在主机上填写独立部署配置，目录示例见 [examples/deployment.json](examples/deployment.json)：

```bash
./hammermigrate deploy --profile /path/deployment.json --dry-run
./hammermigrate deploy --profile /path/deployment.json
./hammermigrate check --profile /path/deployment.json
```

源码部署会先构建并验证；发布包直接使用随包 JAR。容器仅需 Java 21+、Bash 和共享目录。部署工具安装 Python 运行时、各服独立 JAR、共享启动器及节点脚本。将容器原启动命令接入节点脚本即可：

```bash
bash /data/start-hammermigrate.sh java -jar server.jar --nogui
```

升级不重启服务器。部署写入失败会自动回退，强制中断后也可按部署 ID 恢复。完整说明见 [部署文档](docs/deployment.md)。

## 使用与验证

```text
/hmigrate dry-run OldName NewName --premium off
/hmigrate queue OldName NewName --premium off
/hmigrate status
```

目标账号已有数据时必须明确指定 `--merge`。支持的积分合并累加余额；粒子组保留 UUID 和效果引用；其他冲突默认以旧账号为主，未适配的冲突阻止执行。

自动发现有明确边界。只有实际扫描范围内的完整报告才能证明覆盖；跳过异常、未知字段或外部数据源都需要审阅。详见 [迁移操作](docs/migration.md) 和 [验证范围](docs/verification.md)。

## 项目结构

```text
HammerMigrate/
├── hammermigrate       统一命令入口
├── bin/                部署、启动与运行时工具
├── examples/           使用通用名称和路径的示例配置
├── docs/               部署、迁移、验证和实现说明
├── src/main/           Java 平台入口及 Python 引擎
├── tests/              独立回归与故障注入
├── target/             Maven 构建产物
└── dist/               可分发发布包
```

源码仓库提供完整验证与发布构建；发布包使用 `deploy` 和 `check`。

```bash
./hammermigrate verify
./hammermigrate package
```
