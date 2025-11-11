import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import os
import threading
from pathlib import Path

class XlsxToCsvConverter:
    def __init__(self, root):
        self.root = root
        self.root.title("XLSX转CSV工具")
        self.root.geometry("600x400")
        self.root.resizable(True, True)
        
        # 设置样式
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # 文件路径变量
        self.selected_file_path = tk.StringVar()
        self.output_path = tk.StringVar()
        
        self.setup_ui()
        
    def setup_ui(self):
        # 主框架
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # 标题
        title_label = ttk.Label(main_frame, text="XLSX转CSV工具", 
                               font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # 文件选择区域
        file_frame = ttk.LabelFrame(main_frame, text="选择文件", padding="10")
        file_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        file_frame.columnconfigure(1, weight=1)
        
        ttk.Label(file_frame, text="XLSX文件:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        
        file_entry = ttk.Entry(file_frame, textvariable=self.selected_file_path, width=50)
        file_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        
        browse_button = ttk.Button(file_frame, text="浏览", command=self.browse_file)
        browse_button.grid(row=0, column=2)
        
        # 输出设置区域
        output_frame = ttk.LabelFrame(main_frame, text="输出设置", padding="10")
        output_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        output_frame.columnconfigure(1, weight=1)
        
        ttk.Label(output_frame, text="输出目录:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        
        output_entry = ttk.Entry(output_frame, textvariable=self.output_path, width=50)
        output_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        
        output_button = ttk.Button(output_frame, text="选择目录", command=self.browse_output_dir)
        output_button.grid(row=0, column=2)
        
        # 转换选项区域
        options_frame = ttk.LabelFrame(main_frame, text="转换选项", padding="10")
        options_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # 编码选择
        ttk.Label(options_frame, text="编码格式:").grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.encoding_var = tk.StringVar(value="utf-8-sig")
        encoding_combo = ttk.Combobox(options_frame, textvariable=self.encoding_var, 
                                     values=["utf-8-sig", "utf-8", "gbk", "gb2312"], 
                                     state="readonly", width=15)
        encoding_combo.grid(row=0, column=1, sticky=tk.W, padx=(0, 20))
        
        # 分隔符选择
        ttk.Label(options_frame, text="分隔符:").grid(row=0, column=2, sticky=tk.W, padx=(0, 10))
        self.separator_var = tk.StringVar(value=",")
        separator_combo = ttk.Combobox(options_frame, textvariable=self.separator_var, 
                                      values=[",", ";", "\t"], 
                                      state="readonly", width=10)
        separator_combo.grid(row=0, column=3, sticky=tk.W)
        
        # 进度条
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(main_frame, variable=self.progress_var, 
                                           maximum=100, mode='determinate')
        self.progress_bar.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # 状态标签
        self.status_label = ttk.Label(main_frame, text="就绪", foreground="blue")
        self.status_label.grid(row=5, column=0, columnspan=3, pady=(0, 10))
        
        # 按钮区域
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=6, column=0, columnspan=3, pady=(0, 10))
        
        self.convert_button = ttk.Button(button_frame, text="开始转换", 
                                        command=self.start_conversion, style="Accent.TButton")
        self.convert_button.pack(side=tk.LEFT, padx=(0, 10))
        
        clear_button = ttk.Button(button_frame, text="清空", command=self.clear_all)
        clear_button.pack(side=tk.LEFT, padx=(0, 10))
        
        exit_button = ttk.Button(button_frame, text="退出", command=self.root.quit)
        exit_button.pack(side=tk.LEFT)
        
        # 日志区域
        log_frame = ttk.LabelFrame(main_frame, text="转换日志", padding="10")
        log_frame.grid(row=7, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(10, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        main_frame.rowconfigure(7, weight=1)
        
        # 创建文本框和滚动条
        self.log_text = tk.Text(log_frame, height=8, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
    def browse_file(self):
        """浏览并选择XLSX文件"""
        file_path = filedialog.askopenfilename(
            title="选择XLSX文件",
            filetypes=[("Excel文件", "*.xlsx"), ("Excel文件", "*.xls"), ("所有文件", "*.*")]
        )
        if file_path:
            self.selected_file_path.set(file_path)
            # 自动设置输出目录为文件所在目录
            output_dir = os.path.dirname(file_path)
            self.output_path.set(output_dir)
            self.log_message(f"已选择文件: {file_path}")
            
    def browse_output_dir(self):
        """选择输出目录"""
        output_dir = filedialog.askdirectory(title="选择输出目录")
        if output_dir:
            self.output_path.set(output_dir)
            self.log_message(f"输出目录设置为: {output_dir}")
            
    def log_message(self, message):
        """在日志区域添加消息"""
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)
        self.root.update_idletasks()
        
    def start_conversion(self):
        """开始转换过程"""
        if not self.selected_file_path.get():
            messagebox.showerror("错误", "请先选择XLSX文件！")
            return
            
        if not self.output_path.get():
            messagebox.showerror("错误", "请选择输出目录！")
            return
            
        # 禁用转换按钮
        self.convert_button.config(state="disabled")
        
        # 在新线程中执行转换
        thread = threading.Thread(target=self.convert_file)
        thread.daemon = True
        thread.start()
        
    def convert_file(self):
        """执行文件转换"""
        try:
            input_file = self.selected_file_path.get()
            output_dir = self.output_path.get()
            encoding = self.encoding_var.get()
            separator = self.separator_var.get()
            
            self.status_label.config(text="正在读取Excel文件...", foreground="orange")
            self.progress_var.set(20)
            self.log_message("开始转换过程...")
            
            # 读取Excel文件
            self.log_message(f"正在读取文件: {input_file}")
            excel_file = pd.ExcelFile(input_file)
            
            # 获取所有工作表名称
            sheet_names = excel_file.sheet_names
            self.log_message(f"发现 {len(sheet_names)} 个工作表: {', '.join(sheet_names)}")
            
            self.progress_var.set(40)
            self.status_label.config(text="正在转换数据...", foreground="orange")
            
            # 转换每个工作表
            converted_files = []
            for i, sheet_name in enumerate(sheet_names):
                self.log_message(f"正在转换工作表: {sheet_name}")
                
                # 读取工作表数据
                df = pd.read_excel(input_file, sheet_name=sheet_name)
                
                # 生成输出文件名
                base_name = Path(input_file).stem
                if len(sheet_names) > 1:
                    output_filename = f"{base_name}_{sheet_name}.csv"
                else:
                    output_filename = f"{base_name}.csv"
                    
                output_file = os.path.join(output_dir, output_filename)
                
                # 保存为CSV
                df.to_csv(output_file, 
                         encoding=encoding, 
                         sep=separator, 
                         index=False, 
                         na_rep='')
                
                converted_files.append(output_file)
                self.log_message(f"已保存: {output_filename}")
                
                # 更新进度
                progress = 40 + (i + 1) * 50 / len(sheet_names)
                self.progress_var.set(progress)
                
            self.progress_var.set(100)
            self.status_label.config(text="转换完成！", foreground="green")
            self.log_message(f"转换完成！共转换 {len(converted_files)} 个文件")
            
            # 显示成功消息
            messagebox.showinfo("成功", f"转换完成！\n共转换 {len(converted_files)} 个文件\n保存在: {output_dir}")
            
        except Exception as e:
            self.status_label.config(text="转换失败！", foreground="red")
            error_msg = f"转换过程中出现错误: {str(e)}"
            self.log_message(error_msg)
            messagebox.showerror("错误", error_msg)
            
        finally:
            # 重新启用转换按钮
            self.convert_button.config(state="normal")
            
    def clear_all(self):
        """清空所有输入和日志"""
        self.selected_file_path.set("")
        self.output_path.set("")
        self.log_text.delete(1.0, tk.END)
        self.progress_var.set(0)
        self.status_label.config(text="就绪", foreground="blue")
        self.log_message("已清空所有内容")

def main():
    root = tk.Tk()
    app = XlsxToCsvConverter(root)
    root.mainloop()

if __name__ == "__main__":
    main() 