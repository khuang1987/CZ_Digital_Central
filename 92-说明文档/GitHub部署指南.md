# GitHub Pages 部署指南

本文档说明如何将 MkDocs 说明书部署到 GitHub Pages。

**最后更新：** 2025-11-10

---

## 📋 前置要求

- ✅ GitHub 账号
- ✅ 仓库已初始化 Git
- ✅ 已安装 Git
- ✅ 已安装 Python 和 MkDocs

---

## 🚀 快速部署步骤

### 步骤 1：检查 Git 仓库状态

在项目根目录（`250418_MDDAP_project`）执行：

```bash
git status
```

确认：
- ✅ 仓库已初始化
- ✅ 所有更改已提交（或准备提交）

---

### 步骤 2：构建静态网站

在 `92-说明文档` 目录执行：

```bash
cd 92-说明文档
mkdocs build --clean
```

**检查构建结果：**
- ✅ 查看 `site` 文件夹是否生成
- ✅ 检查是否有错误或警告

---

### 步骤 3：配置 GitHub Pages

#### 方法 A：使用 gh-deploy（推荐）

```bash
cd 92-说明文档
mkdocs gh-deploy
```

**这个命令会：**
1. 自动构建网站
2. 创建 `gh-pages` 分支
3. 推送到 GitHub
4. 配置 GitHub Pages

---

#### 方法 B：手动部署

如果 `gh-deploy` 失败，可以手动部署：

```bash
# 1. 构建网站
cd 92-说明文档
mkdocs build --clean

# 2. 切换到 gh-pages 分支（如果存在）
git checkout gh-pages

# 或创建新分支
git checkout --orphan gh-pages
git rm -rf .

# 3. 复制 site 文件夹内容到根目录
cp -r site/* .

# 4. 提交
git add .
git commit -m "Deploy documentation to GitHub Pages"

# 5. 推送到 GitHub
git push origin gh-pages

# 6. 切换回主分支
git checkout main
```

---

### 步骤 4：配置 GitHub Pages 设置

1. 打开 GitHub 仓库页面
2. 进入 **Settings** → **Pages**
3. 设置：
   - **Source**: `gh-pages` 分支
   - **Folder**: `/ (root)`
4. 点击 **Save**

---

### 步骤 5：访问网站

部署完成后，访问：

```
https://您的用户名.github.io/250418_MDDAP_project/
```

或如果使用自定义域名：

```
https://您的自定义域名/
```

---

## 🔧 配置 mkdocs.yml

确保 `mkdocs.yml` 中已配置 `site_url`：

```yaml
site_name: CZ Ops 数字化数据平台 - 电子说明书
site_url: https://您的用户名.github.io/250418_MDDAP_project/
```

---

## ⚠️ 常见问题

### 问题 1：网络连接失败

**错误信息：**
```
fatal: unable to access 'https://github.com/...': Recv failure: Connection was reset
```

**解决方法：**

1. **检查网络连接**
   ```bash
   ping github.com
   ```

2. **使用 SSH 而不是 HTTPS**
   ```bash
   git remote set-url origin git@github.com:用户名/仓库名.git
   ```

3. **配置代理（如果在公司网络）**
   ```bash
   git config --global http.proxy http://proxy.company.com:8080
   git config --global https.proxy https://proxy.company.com:8080
   ```

4. **重试部署**
   ```bash
   mkdocs gh-deploy --force
   ```

---

### 问题 2：权限错误

**错误信息：**
```
Permission denied (publickey)
```

**解决方法：**

1. **配置 SSH 密钥**
   - 生成 SSH 密钥：`ssh-keygen -t ed25519 -C "your_email@example.com"`
   - 添加到 GitHub：Settings → SSH and GPG keys

2. **或使用 Personal Access Token**
   - GitHub Settings → Developer settings → Personal access tokens
   - 创建 token 并用于认证

---

### 问题 3：构建警告

**警告信息：**
```
WARNING - Doc file '...' contains a link '...', but the target is not found
```

**解决方法：**

这些警告不会阻止部署，但建议修复：
1. 检查链接是否正确
2. 确认目标文件存在
3. 修复锚点链接

---

### 问题 4：页面 404

**原因：**
- `site_url` 配置不正确
- 文件路径问题

**解决方法：**

1. **检查 `site_url` 配置**
   ```yaml
   site_url: https://您的用户名.github.io/250418_MDDAP_project/
   ```

2. **重新构建和部署**
   ```bash
   mkdocs build --clean
   mkdocs gh-deploy --force
   ```

---

## 📝 自动化部署（可选）

### 使用 GitHub Actions

创建 `.github/workflows/docs.yml`：

```yaml
name: Deploy Documentation

on:
  push:
    branches:
      - main
    paths:
      - '92-说明文档/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          cd 92-说明文档
          pip install -r requirements.txt
      
      - name: Deploy to GitHub Pages
        run: |
          cd 92-说明文档
          mkdocs gh-deploy --force
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

---

## 🔄 更新文档

每次更新文档后：

```bash
# 1. 提交更改
git add .
git commit -m "Update documentation"

# 2. 推送到 GitHub
git push origin main

# 3. 部署到 GitHub Pages
cd 92-说明文档
mkdocs gh-deploy
```

---

## ✅ 部署检查清单

- [ ] Git 仓库已初始化
- [ ] 所有更改已提交
- [ ] `mkdocs.yml` 配置正确
- [ ] 构建成功（无错误）
- [ ] GitHub Pages 已配置
- [ ] 网站可以访问
- [ ] 所有链接正常工作
- [ ] 搜索功能正常

---

## 📚 相关资源

- [MkDocs 文档](https://www.mkdocs.org/)
- [GitHub Pages 文档](https://docs.github.com/en/pages)
- [Material for MkDocs 文档](https://squidfunk.github.io/mkdocs-material/)

---

**最后更新：** 2025-11-10

