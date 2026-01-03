#!/usr/bin/env bun

import { echo } from "zx";
import {
  readFileSync,
  writeFileSync,
  mkdirSync,
  existsSync,
  readdirSync,
  unlinkSync,
  rmdirSync,
} from "fs";
import { join } from "path";

// 配置常量
const ROOT = import.meta.dirname;
const DOC_DIR = join(ROOT, "doc");
const API_MD = join(DOC_DIR, "api.md");
const TEMP_DIR = "/tmp/jdb_doc";
const JSON_OUT = join(DOC_DIR, "api.json");
const PACKAGES = ["jdb", "jdb_alloc", "jdb_fs", "jdb_proto", "jdb_trait"];

// 工具函数
const ensureDir = (path) => {
  if (!existsSync(path)) mkdirSync(path, { recursive: true });
};

const init = () => {
  ensureDir(DOC_DIR);
  ensureDir(TEMP_DIR);
};

init();

const tempFiles = [
  ...PACKAGES.map((pkg) => join(TEMP_DIR, `${pkg}.json`)),
  JSON_OUT,
];

/**
 * 生成文档 JSON
 */
async function generateDocJson() {
  echo("使用源代码分析方法生成文档...");
  await generateFromSource();
}

/**
 * 解析生成的 JSON 文档
 */
async function parseGeneratedJson() {
  echo("解析 JSON 文档...");
  const apiData = [];

  for (const pkg of PACKAGES) {
    const jsonPath = join(TEMP_DIR, `${pkg}.json`);
    if (existsSync(jsonPath)) {
      const pkgInfo = parsePackageDoc(pkg, jsonPath);
      apiData.push(pkgInfo);
      echo(`解析包 ${pkg} 完成`);
    } else {
      echo(`包 ${pkg} 的 JSON 文档不存在`);
    }
  }

  writeFileSync(JSON_OUT, JSON.stringify(apiData, null, 2));
  echo(`合并的 JSON 已保存到: ${JSON_OUT}`);

  echo("生成 Markdown 文档...");
  const markdown = generateApiMarkdown(apiData);
  writeFileSync(API_MD, markdown);
  echo(`API 文档已生成: ${API_MD}`);
}

/**
 * 解析单个包的文档
 */
function parsePackageDoc(pkg, jsonPath) {
  try {
    const content = readFileSync(jsonPath, "utf8");
    const doc = JSON.parse(content);

    const pkgInfo = {
      name: pkg,
      description: doc.crate?.description || "",
      version: doc.crate?.version || "",
      traits: [],
      structs: [],
      functions: [],
      constants: [],
      types: [],
    };

    if (doc.index) {
      for (const [key, item] of Object.entries(doc.index)) {
        if (item.visibility === "public") {
          switch (item.kind) {
            case "trait":
              pkgInfo.traits.push(parseTrait(doc, item));
              break;
            case "struct":
              pkgInfo.structs.push(parseStruct(doc, item));
              break;
            case "function":
              pkgInfo.functions.push(parseFunction(doc, item));
              break;
            case "const":
              pkgInfo.constants.push(parseConstant(doc, item));
              break;
            case "type":
              pkgInfo.types.push(parseTypeAlias(doc, item));
              break;
          }
        }
      }
    }

    return pkgInfo;
  } catch (error) {
    echo(`解析包 ${pkg} 文档失败: ${error.message}`);
    return {
      name: pkg,
      description: "",
      version: "",
      traits: [],
      structs: [],
      functions: [],
      constants: [],
      types: [],
      error: error.message,
    };
  }
}

/**
 * 解析 Trait
 */
function parseTrait(doc, item) {
  const trait = {
    name: item.name,
    docs: getItemDocs(doc, item),
    methods: [],
    associated_types: [],
    required_methods: [],
    provided_methods: [],
  };

  if (item.inner && doc.index[item.inner]) {
    const inner = doc.index[item.inner];
    if (inner.items) {
      for (const methodId of inner.items) {
        if (doc.index[methodId]) {
          const method = doc.index[methodId];
          const methodInfo = {
            name: method.name,
            docs: getItemDocs(doc, method),
            signature: getItemSignature(doc, method),
            generics: getItemGenerics(doc, method),
            where_clause: getItemWhereClause(doc, method),
          };

          if (method.kind === "method" && method.default) {
            trait.provided_methods.push(methodInfo);
          } else if (method.kind === "method") {
            trait.required_methods.push(methodInfo);
          } else if (method.kind === "assoc_type") {
            trait.associated_types.push({
              name: method.name,
              docs: getItemDocs(doc, method),
              bounds: getItemBounds(doc, method),
            });
          }
        }
      }
    }
  }

  return trait;
}

/**
 * 解析 Struct
 */
function parseStruct(doc, item) {
  const struct = {
    name: item.name,
    docs: getItemDocs(doc, item),
    fields: [],
    methods: [],
    implementations: [],
  };

  if (item.inner && doc.index[item.inner]) {
    const inner = doc.index[item.inner];

    if (inner.fields) {
      for (const fieldId of inner.fields) {
        if (doc.index[fieldId]) {
          const field = doc.index[fieldId];
          struct.fields.push({
            name: field.name,
            docs: getItemDocs(doc, field),
            type: getFieldType(doc, field),
          });
        }
      }
    }

    if (inner.items) {
      for (const methodId of inner.items) {
        if (doc.index[methodId]?.visibility === "public") {
          const method = doc.index[methodId];
          if (method.kind === "method") {
            struct.methods.push({
              name: method.name,
              docs: getItemDocs(doc, method),
              signature: getItemSignature(doc, method),
              generics: getItemGenerics(doc, method),
              where_clause: getItemWhereClause(doc, method),
            });
          }
        }
      }
    }
  }

  return struct;
}

/**
 * 解析函数
 */
function parseFunction(doc, item) {
  return {
    name: item.name,
    docs: getItemDocs(doc, item),
    signature: getItemSignature(doc, item),
    generics: getItemGenerics(doc, item),
    where_clause: getItemWhereClause(doc, item),
  };
}

/**
 * 解析常量
 */
function parseConstant(doc, item) {
  return {
    name: item.name,
    docs: getItemDocs(doc, item),
    type: getConstantType(doc, item),
    value: getConstantValue(doc, item),
  };
}

/**
 * 解析类型别名
 */
function parseTypeAlias(doc, item) {
  return {
    name: item.name,
    docs: getItemDocs(doc, item),
    type: getTypeAliasType(doc, item),
  };
}

/**
 * 过滤中文注释
 */
function filterChinese(comments) {
  if (!comments) return "";

  return comments
    .split("\n")
    .map((line) => {
      if (!line.trim()) return "";

      if (/[\u4e00-\u9fa5]/.test(line)) {
        return line.trim();
      }

      const parts = line.split("/");
      const chineseParts = parts.filter((part) => /[\u4e00-\u9fa5]/.test(part));
      return chineseParts.length > 0 ? chineseParts.join("/") : "";
    })
    .filter((line) => line.trim() !== "")
    .join("\n");
}

/**
 * 获取项的文档
 */
function getItemDocs(doc, item) {
  return item.docs && doc.index[item.docs]
    ? filterChinese(doc.index[item.docs])
    : "";
}

/**
 * 获取项的签名
 */
function getItemSignature(doc, item) {
  return item.signature && doc.index[item.signature]
    ? doc.index[item.signature]
    : "";
}

/**
 * 获取项的泛型参数
 */
function getItemGenerics(doc, item) {
  return item.generics && doc.index[item.generics]
    ? doc.index[item.generics]
    : [];
}

/**
 * 获取项的 where 子句
 */
function getItemWhereClause(doc, item) {
  return item.where_clause && doc.index[item.where_clause]
    ? doc.index[item.where_clause]
    : null;
}

/**
 * 获取字段类型
 */
function getFieldType(doc, field) {
  return field.type && doc.index[field.type] ? doc.index[field.type] : "";
}

/**
 * 获取常量类型
 */
function getConstantType(doc, constant) {
  return constant.type && doc.index[constant.type]
    ? doc.index[constant.type]
    : "";
}

/**
 * 获取常量值
 */
function getConstantValue(doc, constant) {
  return constant.value && doc.index[constant.value]
    ? doc.index[constant.value]
    : "";
}

/**
 * 获取类型别名类型
 */
function getTypeAliasType(doc, typeAlias) {
  return typeAlias.type && doc.index[typeAlias.type]
    ? doc.index[typeAlias.type]
    : "";
}

/**
 * 获取项的约束
 */
function getItemBounds(doc, item) {
  return item.bounds && doc.index[item.bounds] ? doc.index[item.bounds] : [];
}

/**
 * 合并多行签名为一行
 */
function mergeSignature(signature) {
  if (!signature) return "";

  return signature
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line)
    .join(" ");
}

/**
 * 统一排版函数
 * - 最多2个连续空行
 * - # 之前必须有空行
 */
function formatMarkdown(markdown) {
  // 1. 将多个连续空行替换为最多2个
  markdown = markdown.replace(/\n{3,}/g, "\n\n");

  // 2. 确保 # 标题前有空行
  markdown = markdown.replace(/([^\n])\n(#+)/g, "$1\n\n$2");

  // 3. 确保文档开头没有多余空行
  markdown = markdown.trimStart();

  // 4. 确保文档末尾只有一个换行
  markdown = markdown.trimEnd() + "\n";

  return markdown;
}

/**
 * 生成 API Markdown 文档
 */
function generateApiMarkdown(apiData) {
  let markdown = "# JDB 已经实现模块的公开接口";

  for (const pkg of apiData) {
    markdown += generatePackageMarkdown(pkg);
  }

  // 应用统一排版
  return formatMarkdown(markdown);
}

/**
 * 生成单个包的 Markdown 文档
 */
function generatePackageMarkdown(pkg) {
  let markdown = `## ${pkg.name}

${pkg.description ? `> ${pkg.description}\n` : ""}

`;

  if (pkg.traits?.length > 0) {
    markdown += `### 特征\n`;
    for (const trait of pkg.traits) {
      markdown += generateTraitMarkdown(trait);
    }
  }

  if (pkg.structs?.length > 0) {
    markdown += `### 结构体\n`;
    for (const struct of pkg.structs) {
      markdown += generateStructMarkdown(struct);
    }
  }

  if (pkg.functions?.length > 0) {
    markdown += `### 函数\n`;
    markdown += "\n```rust\n";
    for (const fn of pkg.functions) {
      markdown += generateFunctionMarkdown(fn);
    }
    markdown += "```\n";
  }

  if (pkg.constants?.length > 0) {
    markdown += `### 常量\n`;
    for (const constant of pkg.constants) {
      markdown += generateConstantMarkdown(constant);
    }
  }

  if (pkg.types?.length > 0) {
    markdown += `### 类型别名\n`;
    for (const typeAlias of pkg.types) {
      markdown += generateTypeAliasMarkdown(typeAlias);
    }
  }

  markdown += "\n---\n";
  return markdown;
}

/**
 * 生成 Trait 的 Markdown 文档
 */
function generateTraitMarkdown(trait) {
  let markdown = `#### \`${trait.name}\`\n`;

  if (trait.docs?.trim()) {
    markdown += `\n${trait.docs.trim()}\n`;
  }

  if (trait.associated_types?.length > 0) {
    markdown += `\n**关联类型**\n`;
    for (const assocType of trait.associated_types) {
      markdown += `- \`${assocType.name}\``;
      if (assocType.bounds?.length > 0) {
        markdown += `: ${assocType.bounds.join(" + ")}`;
      }
      if (assocType.docs) {
        markdown += ` - ${assocType.docs}`;
      }
      markdown += "\n";
    }
  }

  if (trait.required_methods?.length > 0) {
    markdown += "\n```rust\n";
    for (const method of trait.required_methods) {
      markdown += generateMethodMarkdown(method);
    }
    markdown += "```\n";
  }

  if (trait.provided_methods?.length > 0) {
    markdown += `\n**默认提供的方法**\n`;
    markdown += "\n```rust\n";
    for (const method of trait.provided_methods) {
      markdown += generateMethodMarkdown(method);
    }
    markdown += "```\n";
  }

  return markdown;
}

/**
 * 生成 Struct 的 Markdown 文档
 */
function generateStructMarkdown(struct) {
  let markdown = `#### \`${struct.name}\`\n`;

  if (struct.docs?.trim()) {
    markdown += `\n${struct.docs.trim()}\n`;
  }

  if (struct.fields?.length > 0) {
    markdown += `\n**字段**\n`;
    for (const field of struct.fields) {
      markdown += `- \`${field.name}: ${field.type}\``;
      if (field.docs) {
        markdown += ` - ${field.docs}`;
      }
      markdown += "\n";
    }
  }

  if (struct.methods?.length > 0) {
    markdown += `\n**方法**\n`;
    markdown += "\n```rust\n";
    for (const method of struct.methods) {
      markdown += generateMethodMarkdown(method);
    }
    markdown += "```\n";
  }

  return markdown;
}

/**
 * 生成方法的 Markdown 文档
 */
function generateMethodMarkdown(method) {
  let markdown = "";

  if (method.docs?.trim()) {
    markdown += `// ${method.docs.trim()}\n`;
  }

  if (method.signature) {
    markdown += `${mergeSignature(method.signature)}\n\n`;
  }

  return markdown;
}

/**
 * 生成函数的 Markdown 文档
 */
function generateFunctionMarkdown(fn) {
  let markdown = "";

  if (fn.docs?.trim()) {
    markdown += `// ${fn.docs.trim()}\n`;
  }

  if (fn.signature) {
    markdown += `${mergeSignature(fn.signature)}\n\n`;
  }

  return markdown;
}

/**
 * 生成常量的 Markdown 文档
 */
function generateConstantMarkdown(constant) {
  let markdown = `#### \`${constant.name}\`\n`;

  if (constant.docs?.trim()) {
    markdown += `\n${constant.docs.trim()}\n`;
  }

  markdown += `\n**类型**: \`${constant.type}\`\n`;
  markdown += `**值**: \`${constant.value}\`\n`;

  return markdown;
}

/**
 * 生成类型别名的 Markdown 文档
 */
function generateTypeAliasMarkdown(typeAlias) {
  let markdown = `#### \`${typeAlias.name}\`\n`;

  if (typeAlias.docs?.trim()) {
    markdown += `\n${typeAlias.docs.trim()}\n`;
  }

  markdown += `\n**类型**: \`${typeAlias.type}\`\n`;

  return markdown;
}

/**
 * 从源代码生成文档
 */
async function generateFromSource() {
  echo("从源代码生成文档...");

  const apiData = [];

  for (const pkg of PACKAGES) {
    echo(`分析包: ${pkg}`);
    const pkgInfo = await analyzePackage(pkg);
    apiData.push(pkgInfo);
    echo(`分析包 ${pkg} 完成`);
  }

  writeFileSync(JSON_OUT, JSON.stringify(apiData, null, 2));
  echo(`合并的 JSON 已保存到: ${JSON_OUT}`);

  echo("生成 Markdown 文档...");
  const markdown = generateApiMarkdown(apiData);
  writeFileSync(API_MD, markdown);
  echo(`API 文档已生成: ${API_MD}`);

  echo("文档生成完成!");
}

/**
 * 分析包
 */
async function analyzePackage(pkgName) {
  const pkgPath = join(ROOT, pkgName);
  const cargoTomlPath = join(pkgPath, "Cargo.toml");
  const libRsPath = join(pkgPath, "src", "lib.rs");

  const pkgInfo = {
    name: pkgName,
    description: "",
    version: "",
    traits: [],
    structs: [],
    functions: [],
    constants: [],
    types: [],
    seenFunctions: new Set(),
  };

  try {
    if (existsSync(cargoTomlPath)) {
      const cargoToml = readFileSync(cargoTomlPath, "utf8");
      const descMatch = cargoToml.match(/description\s*=\s*"([^"]+)"/);
      const versionMatch = cargoToml.match(/version\s*=\s*"([^"]+)"/);

      if (descMatch) pkgInfo.description = descMatch[1];
      if (versionMatch) pkgInfo.version = versionMatch[1];
    }

    if (existsSync(libRsPath)) {
      await analyzeSource(libRsPath, pkgInfo);
    }

    const srcDir = join(pkgPath, "src");
    if (existsSync(srcDir)) {
      const file_li = readdirSync(srcDir);

      for (const file of file_li) {
        if (file.endsWith(".rs") && file !== "lib.rs") {
          const filePath = join(srcDir, file);
          await analyzeSource(filePath, pkgInfo);
        }
      }
    }
  } catch (error) {
    echo(`分析包 ${pkgName} 时出错: ${error.message}`);
    pkgInfo.error = error.message;
  }

  return pkgInfo;
}

/**
 * 分析 Rust 源文件
 */
async function analyzeSource(filePath, pkg) {
  try {
    const content = readFileSync(filePath, "utf8");
    const lines = content.split("\n");

    const parser = new RustParser(pkg);
    parser.parse(lines);
  } catch (error) {
    echo(`分析文件 ${filePath} 时出错: ${error.message}`);
  }
}

/**
 * Rust 源代码解析器
 */
class RustParser {
  constructor(pkg) {
    this.pkg = pkg;
    this.docs = [];
    this.inDoc = false;
    this.ctx = { trait: false, struct: false, impl: false, fn: false };
    this.cur = { trait: null, struct: null, impl: null, fn: null };
    this.braceCount = 0;
    this.fnLines = [];
  }

  parse(lines) {
    for (const line of lines) {
      this.processLine(line.trim());
    }
  }

  processLine(line) {
    if (line.startsWith("///")) {
      this.handleDocComment(line);
      return;
    } else if (this.inDoc && this.shouldEndDoc(line)) {
      this.inDoc = false;
    }

    // 优先处理正在解析的上下文
    if (this.ctx.fn) this.continueFunction(line);
    else if (this.ctx.struct) this.handleStructField(line);
    else if (line.startsWith("pub trait ")) this.handleTraitDef(line);
    else if (line.startsWith("pub struct ")) this.handleStructDef(line);
    else if (line.startsWith("impl ")) this.handleImplDef(line);
    else if (line.startsWith("pub const ")) this.handleConstDef(line);
    else if (line.startsWith("pub type ")) this.handleTypeDef(line);
    else if (line.startsWith("pub fn ")) this.handleFunctionDef(line);
    else if (this.ctx.trait && line.match(/^\s*fn\s+/))
      this.handleFunctionDef(line);
    else if (line === "}") this.handleEndBlock(line);
  }

  handleDocComment(line) {
    const comment = filterChinese(line.substring(3).trim());
    if (comment) this.docs.push(comment);
    this.inDoc = true;
  }

  shouldEndDoc(line) {
    return (
      line.startsWith("pub") ||
      line.startsWith("impl") ||
      line.startsWith("fn") ||
      line === "}"
    );
  }

  handleTraitDef(line) {
    const name = line.match(/pub trait\s+(\w+)/)?.[1];
    if (name) {
      this.cur.trait = {
        name,
        docs: this.docs.join("\n"),
        methods: [],
        required_methods: [],
        provided_methods: [],
        seenMethods: new Set(),
      };
      this.ctx.trait = true;
      this.docs = [];
    }
  }

  handleStructDef(line) {
    const name = line.match(/pub struct\s+(\w+)/)?.[1];
    if (name) {
      this.cur.struct = {
        name,
        docs: this.docs.join("\n"),
        fields: [],
        methods: [],
      };
      this.ctx.struct = true;
      this.docs = [];
    }
  }

  handleImplDef(line) {
    const name = line.match(/impl\s+(\w+)/)?.[1];
    if (name) {
      this.cur.impl = { structName: name, methods: [] };
      this.ctx.impl = true;
    }
  }

  handleConstDef(line) {
    const match = line.match(/pub const\s+(\w+):\s*(\w+)\s*=\s*([^;]+)/);
    if (match) {
      this.pkg.constants.push({
        name: match[1],
        type: match[2],
        value: match[3].trim(),
        docs: this.docs.join("\n"),
      });
      this.docs = [];
    }
  }

  handleTypeDef(line) {
    const match = line.match(/pub type\s+(\w+)\s*=\s*([^;]+)/);
    if (match) {
      this.pkg.types.push({
        name: match[1],
        type: match[2].trim(),
        docs: this.docs.join("\n"),
      });
      this.docs = [];
    }
  }

  handleFunctionDef(line) {
    const match = line.match(/^(\s*pub\s+)?fn\s+(\w+)/);
    if (!match) return;

    if (this.ctx.trait && !this.ctx.fn) {
      this.startTraitMethod(line, match[2]);
    } else if (this.ctx.impl && !this.ctx.fn) {
      this.startImplMethod(line, match[2]);
    } else if (
      !this.ctx.trait &&
      !this.ctx.struct &&
      !this.ctx.impl &&
      !this.ctx.fn
    ) {
      this.startStandaloneFunction(line, match[2]);
    }
  }

  startStandaloneFunction(line, name) {
    this.ctx.fn = true;
    this.cur.fn = {
      name,
      docs: this.docs.join("\n"),
      signature: line,
    };
    this.fnLines = [line];
    this.braceCount = this.countBraces(line);
    this.docs = [];

    if (line.endsWith(";")) {
      this.addFunction(this.pkg, this.cur.fn);
      this.resetFn();
    }
  }

  startTraitMethod(line, name) {
    this.ctx.fn = true;
    this.cur.fn = {
      name,
      docs: this.docs.join("\n"),
      signature: line,
    };
    this.fnLines = [line];
    this.braceCount = this.countBraces(line);
    this.docs = [];

    if (line.endsWith(";")) {
      this.addTraitMethod(this.cur.trait, this.cur.fn, true);
      this.resetFn();
    }
  }

  startImplMethod(line, name) {
    this.ctx.fn = true;
    this.cur.fn = {
      name,
      docs: this.docs.join("\n"),
      signature: "",
    };
    this.fnLines = [line];
    this.braceCount = this.countBraces(line);
    this.docs = [];
  }

  continueFunction(line) {
    if (line.startsWith("///")) {
      // 文档注释也要收集
      this.fnLines.push(line);
      return;
    }

    const newFnMatch = line.match(/^(\s*pub\s+)?fn\s+(\w+)/);

    // 收集所有行
    this.fnLines.push(line);

    // 对于 trait 方法，只有在遇到分号结尾或新函数定义时才结束
    if (this.ctx.trait) {
      if (line.trim().endsWith(";")) {
        this.finishCurrentFunction();
      } else if (newFnMatch && this.fnLines.length > 1) {
        // 遇到新函数定义，结束当前函数
        this.finishCurrentFunction();
        this.handleFunctionDef(line);
      }
    } else {
      // 对于非 trait 方法，使用大括号计数
      const braceDelta = this.countBraces(line);
      this.braceCount += braceDelta;

      if (this.braceCount <= 0) {
        this.finishCurrentFunction();
      } else if (newFnMatch && this.braceCount === 0) {
        this.finishCurrentFunction();
        this.handleFunctionDef(line);
      }
    }
  }

  finishCurrentFunction() {
    this.cur.fn.signature = this.fnLines.join("\n");

    if (this.ctx.trait) {
      this.addTraitMethod(
        this.cur.trait,
        this.cur.fn,
        this.fnLines[0].includes(";"),
      );
    } else if (this.ctx.impl) {
      this.addImplMethod(this.pkg, this.cur.impl, this.cur.fn);
    } else {
      this.addFunction(this.pkg, this.cur.fn);
    }

    this.resetFn();
  }

  handleStructField(line) {
    if (!line.includes("pub ") || line.includes("fn")) return;

    const match = line.match(/pub\s+(\w+):\s*([^,}]+)/);
    if (match) {
      this.cur.struct.fields.push({
        name: match[1],
        type: match[2].trim(),
        docs: this.docs.join("\n"),
      });
      this.docs = [];
    }
  }

  handleEndBlock(line) {
    if (this.ctx.trait) {
      this.pkg.traits.push(this.cur.trait);
      this.cur.trait = null;
      this.ctx.trait = false;
    }
    if (this.ctx.struct) {
      this.pkg.structs.push(this.cur.struct);
      this.cur.struct = null;
      this.ctx.struct = false;
    }
    if (this.ctx.impl) {
      this.cur.impl = null;
      this.ctx.impl = false;
    }
  }

  countBraces(str) {
    let open = 0,
      close = 0;
    for (let i = 0; i < str.length; i++) {
      if (str[i] === "{") open++;
      else if (str[i] === "}") close++;
    }
    return open - close;
  }

  addFunction(pkg, fn) {
    if (!pkg.seenFunctions.has(fn.name)) {
      pkg.functions.push(fn);
      pkg.seenFunctions.add(fn.name);
    }
  }

  addTraitMethod(trait, method, required) {
    if (!trait.seenMethods.has(method.name)) {
      (required ? trait.required_methods : trait.provided_methods).push(method);
      trait.seenMethods.add(method.name);
    }
  }

  addImplMethod(pkg, impl, method) {
    let struct = pkg.structs.find((s) => s.name === impl.structName);
    if (!struct) {
      struct = {
        name: impl.structName,
        docs: "",
        fields: [],
        methods: [],
      };
      pkg.structs.push(struct);
    }
    struct.methods.push(method);
  }

  resetFn() {
    this.ctx.fn = false;
    this.cur.fn = null;
    this.fnLines = [];
    this.braceCount = 0;
  }
}

/**
 * 清理临时文件
 */
function cleanup() {
  try {
    for (const file of tempFiles) {
      if (existsSync(file)) {
        unlinkSync(file);
        console.log(`已删除临时文件: ${file}`);
      }
    }

    try {
      const file_li = readdirSync(TEMP_DIR);
      if (file_li.length === 0) {
        rmdirSync(TEMP_DIR);
        console.log(`已删除临时目录: ${TEMP_DIR}`);
      }
    } catch (error) {
      // 忽略目录不为空的错误
    }
  } catch (error) {
    console.warn("清理临时文件时出错:", error.message);
  }
}

/**
 * 主函数
 */
async function main() {
  try {
    console.log("开始生成 JDB API 文档...");

    await generateDocJson();
    cleanup();

    console.log("文档生成完成!");
  } catch (error) {
    console.error("生成文档失败:", error);
    console.error("错误堆栈:", error.stack);

    cleanup();
    process.exit(1);
  }
}

main();

