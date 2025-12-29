用英文（ readme/en.md ）和中文( readme/zh.md )给当前项目撰写 README （先阅读已有内容）
文档要包含本库复刻的原型的链接 https://github.com/sile/scalable_cuckoo_filter

用下面链接分别在性能对比中配图

https://raw.githubusercontent.com/js0-site/jdb/refs/heads/main/autoscale_cuckoo_filter/readme/en.svg
https://raw.githubusercontent.com/js0-site/jdb/refs/heads/main/autoscale_cuckoo_filter/readme/zh.svg

markdown 标题格式： # 项目名 : 项目用途简短有力的口号 (口号不用加句号）
先阅读已有内容，然后再改写
源代码在 src/ 下，演示代码参考 tests/ , 代码缩进用2个空格。
要添加文内目录导航(但无需添加中英文的锚点)。
文档要包含项目功能介绍、使用演示、特性介绍（如有）、设计思路(相关模块的调用流程)、技术堆栈、目录结构，等等
需要对 lib.rs 导出的数据结构和函数做介绍
文档用词要专业不浮夸，表述要简明扼要，尽可能不用量词（避免使用一个、一位）、形容词
多分段优化阅读体验
在最后，可以搜索后，写点技术、项目相关的历史小故事，让内容更加丰富
如果需要画流程图、架构图，请用下面格式：
```mermaid
-graph TD
````
并用 `bash -c 'set -ex && mmdc -i readme/en.md -o /tmp/1.svg && mmdc -i readme/zh.md -o /tmp/2.svg'` 验证是否正确

在 Cargo.toml  中添加 “英 / 中” 双语简洁有力的描述，不用量词(一个等)；添加纯英文的关键(不超过5个)