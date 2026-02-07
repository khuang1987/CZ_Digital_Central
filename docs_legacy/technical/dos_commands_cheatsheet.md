# DOS / CMD 常用命令速查表

本文档记录了 Windows 命令行 (CMD) 中常用的操作指令，方便快速查阅。

## 1. 目录导航 (Navigation)

### `cd` (Change Directory)
切换当前工作目录。

*   **进入同级目录**:
    ```cmd
    cd 文件夹名称
    :: 例如
    cd scripts
    ```

*   **返回上一级目录**:
    ```cmd
    cd ..
    ```

*   **跳转到指定路径**:
    ```cmd
    cd C:\Users\huangk14\Project
    ```

*   **切换盘符**: (注意：直接 `cd D:` 不会切换，需要加 `/d` 或者直接输入盘符)
    ```cmd
    :: 方法1：使用 /d 参数（推荐）
    cd /d D:\Work

    :: 方法2：先切盘符
    D:
    ```

### `dir` (Directory)
列出当前目录下的文件和文件夹。

*   **列出所有内容**:
    ```cmd
    dir
    ```

*   **只显示文件名 (宽列模式)**:
    ```cmd
    dir /w
    ```

## 2. 文件操作 (File Operations)

### `md` 或 `mkdir` (Make Directory)
创建新文件夹。

*   **创建单个文件夹**:
    ```cmd
    md new_folder
    ```

*   **创建多级目录**:
    ```cmd
    md project\src\utils
    ```

### `copy`
复制文件。

*   **复制文件到新位置**:
    ```cmd
    copy source.txt destination_folder\
    ```

### `del`
删除文件。

*   **删除特定文件**:
    ```cmd
    del file.txt
    ```

*   **删除所有 .log 文件**:
    ```cmd
    del *.log
    ```

## 3. 屏幕与系统 (System)

### `cls` (Clear Screen)
清空当前终端屏幕的所有内容（强迫症福音）。
```cmd
cls
```

### `echo`
输出信息。

*   **打印文字**:
    ```cmd
    echo Hello World
    ```

### `whoami`
查看当前登录的用户名称。
```cmd
whoami
```

### `systeminfo`
查看详细的系统信息（OS版本，内存，网络等）。
```cmd
systeminfo
```

## 4. 网络 (Network)

### `ipconfig`
查看本机 IP 地址信息。
```cmd
ipconfig
```

### `ping`
测试网络连通性。
```cmd
ping www.baidu.com
ping 192.168.1.1
```

## 5. 小技巧

*   **自动补全**: 输入文件夹前几个字母，按 `Tab` 键可以自动补全名称。
*   **历史命令**: 按 `↑` (上方向键) 可以调出之前输过的命令。
*   **停止运行**: 按 `Ctrl + C` 可以强制停止正在运行的程序。
