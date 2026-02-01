
import re

FILE_PATH = r"c:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\C_code\CZ_Digital_Central\apps\web_dashboard\src\app\production\labor-eh\page.tsx"

def check_syntax():
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    stack = []
    
    # regex for tags
    tag_regex = re.compile(r'</?(\w+)[^>]*>')
    
    line_num = 0
    for line in lines:
        line_num += 1
        
        # Simple brace counting (ignoring strings/comments for speed, might be noisy)
        for char in line:
            if char == '{':
                stack.append(('{', line_num))
            elif char == '}':
                if not stack or stack[-1][0] != '{':
                    print(f"Error: Unexpected '}}' at line {line_num}")
                else:
                    stack.pop()
            
            elif char == '(':
                stack.append(('(', line_num))
            elif char == ')':
                if not stack or stack[-1][0] != '(':
                    print(f"Error: Unexpected ')' at line {line_num}")
                else:
                    stack.pop()
                    
    if stack:
        print(f"Unclosed items: {stack[:5]} ... total {len(stack)}")
    else:
        print("Braces/Parens Balanced")

if __name__ == "__main__":
    check_syntax()
