# ANN 公开数据集清单

按接入复杂度递增组织，专注于能直接用于 ClickHouse ANN benchmark 的数据集。

## 1. ann-benchmarks.com 全家桶

格式与本仓 `benchmarks/ann_sift1m/data/sift-128-euclidean.hdf5` 一致：
HDF5 文件，含 `train` / `test` / `neighbors` / `distances` 四个 dataset。
对接成本最低 —— 把 `download.sh` 里的 URL 换掉就能跑。

| 数据集 | 维度 | 度量 | 规模 | 备注 |
|---|---|---|---|---|
| MNIST | 784 | L2 | 60K | 极小，调试用 |
| Fashion-MNIST | 784 | L2 | 60K | 同上 |
| **SIFT-1M** | 128 | L2 | 1M | 当前 baseline |
| **GIST-1M** | 960 | L2 | 1M | 高维图像特征，更能压距离计算 |
| **GloVe-25/50/100/200** | 25/50/100/200 | cosine | 1.18M | 词向量，cosine 路径必备 |
| NYTimes | 256 | cosine | 290K | 文本 |
| Last.fm | 65 | cosine | 290K | 协同过滤 |
| **DEEP-image-96** | 96 | cosine | 9.99M | Yandex 图像，10M 量级 |
| Kosarak | 27 983 | Jaccard | 75K | 稀疏 / Jaccard 测试 |

## 2. Big ANN Benchmarks（NeurIPS '21/'23，十亿级）

二进制格式 `.fbin` / `.u8bin` / `.i8bin`，需要单独的 reader。
下行带宽与磁盘是瓶颈：1B 数据集通常 100-400 GB。

| 数据集 | 维度 | 类型 | 规模 | 备注 |
|---|---|---|---|---|
| **BIGANN** | 128 | uint8 / L2 | 1B | SIFT 的 1B 版（INRIA TEXMEX） |
| **DEEP1B** | 96 | float32 / cosine | 1B | Yandex |
| **SPACEV-1B** | 100 | int8 / L2 | 1B | Microsoft Bing |
| **Turing-ANNS-1B** | 100 | float32 / L2 | 1B | Microsoft |
| **Text-to-Image-1B** | 200 | float32 / IP | 1B | Yandex 跨模态，OOD（query 与 base 分布不同） |
| **SimSearchNet++** | 256 | uint8 | 几亿 | Facebook 图像指纹 |

NeurIPS '23 新增赛道：

- **YFCC-10M**：192-d，带标签，测 ANN + 谓词过滤
- **Wikipedia-35M** / **MS MARCO v2**：768-d，BERT 类稠密检索

## 3. LLM 时代的高维 embedding（贴近真实业务）

| 来源 | 维度 | 规模 | 用途 |
|---|---|---|---|
| Cohere `wikipedia-22-12` | 768 | 35M（en）/ 250M（多语种） | RAG，HuggingFace 可下 |
| OpenAI `text-embedding-3-large` | 3072 | 自定 | 高维极端测试（自行 embed 子集） |
| LAION-400M / 2B | 512 (CLIP) | 4-20 亿 | 图文，CC 协议 |
| MS MARCO v2 passage | 768 | 138M | IR / 稠密检索 |
| Arxiv / SPECTER2 | 768 | 几百万 | 学术检索 |

## 4. INRIA TEXMEX 原始 FTP

`.fvecs` / `.bvecs` 格式，SIFT-1M / GIST-1M / SIFT-1B 的最早出处。
FTP 数据通道在多数沙箱里被封，所以本仓走 ann-benchmarks.com 的 HTTPS 镜像。

---

## 数据集选型：按测试目标反推

「公开数据集多」不是答案，**「测哪个轴」才是**。选数据集前先回答：要压什么。

| 测试目标 | 推荐数据集 | 为什么 |
|---|---|---|
| 维度对建图 / 距离计算的成本 | **GIST-1M (960)** 或 Cohere-768 | SIFT-128 太友好，暴露不了高维退化 |
| 非欧度量正确性 | **GloVe (cosine)** 或 **Text-to-Image (IP)** | 只跑 L2 永远验不到 cosine 路径的 bug |
| 分布外 query 的 recall 退化（最像生产） | **Text-to-Image-1B** | BigANN '23 OOD 赛道；query 是文本 embedding，base 是图像 |
| ANN + `WHERE` 过滤的联动 | **YFCC**、**MS MARCO**（带 metadata） | 测 hybrid 过滤实现 |
| 亿级 scale 的索引构建 / 磁盘占用 | **BIGANN-1B** / **DEEP1B** | 但需 200GB+ 存储、24h+ 建索引 |

## 下一步建议

替本仓 DiskANN 挑下一个能暴露问题的 dataset，倾向：

- **GIST-1M（高维 L2，1M 量级）**：本机可跑，建索引 10-20 分钟，比 SIFT-128 更能压距离计算与图度数。
- **GloVe-100-angular**：验 cosine / inner-product 代码路径，规模与 SIFT-1M 接近。

这一组在「能本机跑 + 能暴露未覆盖代码路径」之间取平衡。后续如果要冲到 1B 级，再考虑 BIGANN 或 DEEP1B。

`benchmarks/ann_sift1m/download.sh` 目前只支持单一 URL，扩展时建议参数化为 `--dataset <name>`，对应 ann-benchmarks.com 的命名约定（如 `gist-960-euclidean`、`glove-100-angular`）。
