
import sys

def count_braces(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    brace_count = 0
    paren_count = 0
    bracket_count = 0
    
    for i, line in enumerate(lines):
        for char in line:
            if char == '{': brace_count += 1
            elif char == '}': brace_count -= 1
            elif char == '(': paren_count += 1
            elif char == ')': paren_count -= 1
            elif char == '[': bracket_count += 1
            elif char == ']': bracket_count -= 1
        
        # Check balance every 50 lines or so, or when it hits the return
        if brace_count < 0 or paren_count < 0 or bracket_count < 0:
            print(f"Error at line {i+1}: Negative balance! Braces: {brace_count}, Parens: {paren_count}, Brackets: {bracket_count}")
            # print(f"Line content: {line.strip()}")
            # Reset to zero to find next error
            if brace_count < 0: brace_count = 0
            if paren_count < 0: paren_count = 0
            if bracket_count < 0: bracket_count = 0

    print(f"Final Balance: Braces: {brace_count}, Parens: {paren_count}, Brackets: {bracket_count}")

if __name__ == "__main__":
    count_braces(sys.argv[1])
