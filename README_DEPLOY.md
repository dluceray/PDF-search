
# PDF 合同检索系统（从零部署指引）
## 组成
- 后端：FastAPI（模块2主入口，集成模块4/5）
- 模块 3：本地提取降级
- 模块 4：百度千帆 ModelBuilder v2 接入骨架（需上线前填环境变量）
- 模块 5：多目录索引聚合与检索

## 安装
pip install -r requirements.txt --no-cache-dir
./start.sh
# 健康检查：curl http://127.0.0.1:8000/api/health

## 初始化多目录
POST /api/mdirs/reload 传入 roots / excel_patterns / allowed_exts / schema_hints

## AI 抽取
设置环境变量：QF_API_BASE / QF_AK / QF_SK / QF_MODEL
POST /api/ai/extract 传 { "text": "..." }；失败会回退到本地提取。

## 自启动（可选）
将 pdfsearch.service 放到 /etc/systemd/system/ 并执行 systemctl enable --now pdfsearch。
