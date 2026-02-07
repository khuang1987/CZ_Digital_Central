import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

const getFilesRecursive = (dir: string, baseDir: string): string[] => {
    let results: string[] = [];
    const list = fs.readdirSync(dir);
    list.forEach(file => {
        const fullPath = path.join(dir, file);
        const stat = fs.statSync(fullPath);
        if (stat && stat.isDirectory()) {
            results = results.concat(getFilesRecursive(fullPath, baseDir));
        } else if (file.endsWith('.md')) {
            results.push(path.relative(baseDir, fullPath));
        }
    });
    return results;
};

export async function GET(req: NextRequest) {
    const { searchParams } = new URL(req.url);
    const query = searchParams.get('q')?.toLowerCase();

    if (!query || query.length < 2) {
        return NextResponse.json({ results: [] });
    }

    const docsDir = path.join(process.cwd(), '../../docs');
    const results: any[] = [];

    try {
        const files = getFilesRecursive(docsDir, docsDir);

        for (const file of files) {
            const content = fs.readFileSync(path.join(docsDir, file), 'utf-8');
            const lines = content.split('\n');

            let sectionTitle = '';

            for (let i = 0; i < lines.length; i++) {
                const line = lines[i];
                if (line.startsWith('#')) {
                    sectionTitle = line.replace(/#+\s+/, '').trim();
                }

                if (line.toLowerCase().includes(query)) {
                    const index = line.toLowerCase().indexOf(query);
                    const start = Math.max(0, index - 40);
                    const end = Math.min(line.length, index + query.length + 60);
                    let snippet = line.substring(start, end);

                    if (start > 0) snippet = '...' + snippet;
                    if (end < line.length) snippet = snippet + '...';

                    results.push({
                        file: file, // Now relative path
                        fileName: path.basename(file).replace('.md', '').replace(/^\d+_/, '').replace(/_/g, ' '),
                        section: sectionTitle,
                        snippet: snippet.trim(),
                        line: i + 1
                    });

                    if (results.filter(r => r.file === file).length > 3) break;
                }
            }
        }

        return NextResponse.json({ results: results.slice(0, 15) });
    } catch (error) {
        console.error('Search error:', error);
        return NextResponse.json({ error: 'Failed to perform search' }, { status: 500 });
    }
}
