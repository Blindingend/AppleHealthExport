# 任务目标
作为一个具备代码编写和本地执行能力的 Agent，你的任务是：使用 `pixi` 初始化 Python 环境，编写一个处理超大 XML 文件的 Python 脚本，并直接在当前目录下运行该脚本。最终交付给我可复用的转换代码，以及转换成功的 `.tcx` 或 `.fit` 格式的运动文件集。

# 目录与数据上下文
当前目录下已经存在一个 `apple_health_export/` 文件夹，包含以下源数据：
1. `apple_health_export/export.xml`: 包含了所有的 Apple Health 基础数据。注意：此文件极其庞大（GB 级别），**禁止**将其一次性读入内存。
2. `apple_health_export/workout-routes/`: 包含多次户外运动的 `.gpx` 轨迹文件。

# 详细执行步骤与逻辑要求

## 第一步：环境配置 (使用 Pixi)
1. 在当前目录初始化 pixi 项目：`pixi init`。
2. 添加必需的 Python 依赖（建议至少包含用于流式解析 XML 和处理 GPX/时间序列的库）：`pixi add python lxml`。如果需要其他生成 TCX/FIT 文件的辅助库，请自行评估并添加。

## 第二步：编写转换脚本 (`convert_health.py`)
请编写一个高健壮性的 Python 脚本，满足以下核心业务逻辑：
1. **防止内存溢出 (OOM)：** 必须使用 `lxml.etree.iterparse` 进行流式解析 `export.xml`，边读边清空已处理的节点 (`element.clear()`)。
2. **提取运动主记录：** 解析 `<Workout>` 节点，提取运动类型（如跑步、骑行）、开始时间 (`startDate`)、结束时间 (`endDate`)、总距离和总消耗。过滤掉时长过短或无意义的记录。
3. **提取实时心率：** 在流式解析过程中，捕获 `<Record type="HKQuantityTypeIdentifierHeartRate">` 的时间点和心率值。由于数据是按时间排序的，需将其与对应的 `Workout` 时间窗口进行匹配。
4. **缝合 GPS 轨迹：** 根据 `<Workout>` 中的关联信息（例如通过 `HKObjectRelation` 查找，或直接通过时间戳区间匹配），读取 `workout-routes/` 目录下的对应 `.gpx` 文件。
5. **生成目标文件：** 将运动元数据、GPS 轨迹点和心率序列进行对齐，生成标准的 `.tcx` 或 `.fit` 文件。若某次运动没有 GPS 轨迹（如室内运动），则仅包含时间和心率序列。
6. **输出目录：** 将所有生成的文件保存到当前目录新建的 `output_workouts/` 文件夹中。文件名需包含日期和运动类型，如 `2024-05-06_Running.tcx`。

## 第三步：执行与验证
1. 编写完 `convert_health.py` 后，请直接使用 `pixi run python convert_health.py` 运行它。
2. 捕获并处理运行过程中的任何报错。如果因为 Apple Health 数据结构的特殊性导致报错（如时间戳格式化化问题、缺少关联键等），请自动修改代码并重试。

## 你的最终交付物
1. 执行成功的终端输出总结（成功转换了多少条记录）。
2. 在 `output_workouts/` 目录中生成的结果文件（你只需告诉我生成成功即可，我会在本地查看）。
3. 留存在当前目录下的 `pixi.toml` 和高质量、带有注释的 `convert_health.py` 文件，以便我未来复用。
