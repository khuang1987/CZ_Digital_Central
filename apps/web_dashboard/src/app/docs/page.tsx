import fs from 'fs';
import path from 'path';
import Link from 'next/link';
import { BookOpen, FileText, ChevronRight } from 'lucide-react';
import DocFloatingActions from '@/components/docs/DocFloatingActions';
import SearchDocs from '@/components/docs/SearchDocs';
import GlobalHeaderActions from '@/components/GlobalHeaderActions';

export default async function DocsPage({
    searchParams
}: {
    searchParams: { file?: string }
}) {
    const docsDir = path.join(process.cwd(), '../../docs');

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

    const files = getFilesRecursive(docsDir, docsDir).sort();
    const selectedFile = searchParams.file || files[0];
    let content = '';

    if (selectedFile) {
        const fullFilePath = path.join(docsDir, selectedFile);
        // Security check: ensure path is within docsDir
        if (fullFilePath.startsWith(docsDir) && fs.existsSync(fullFilePath)) {
            content = fs.readFileSync(fullFilePath, 'utf-8');
        }
    }

    // Helper to process inline markdown (backticks, bold, links)
    const processInline = (text: string) => {
        if (!text) return '';
        return text
            .replace(/\*\*(.*?)\*\*/g, '<strong class="text-slate-900 dark:text-slate-100 font-bold">$1</strong>')
            .replace(/`(.*?)`/g, '<code class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-blue-600 dark:text-blue-400 font-mono text-[0.9em] border border-slate-200 dark:border-slate-700">$1</code>');
    };

    // Enhanced markdown to JSX renderer with stateful indentation and collapsible sections
    const renderMarkdown = (text: string) => {
        const lines = text.split('\n');
        const rendered: React.ReactNode[] = [];
        let i = 0;

        while (i < lines.length) {
            const line = lines[i];

            // 1. Level 1 Header (H1)
            if (line.startsWith('# ')) {
                rendered.push(<h1 key={`h1-${i}`} className="text-4xl font-extrabold mb-12 text-slate-900 dark:text-white border-b-2 border-slate-100 dark:border-slate-800 pb-8 tracking-tight" dangerouslySetInnerHTML={{ __html: processInline(line.replace('# ', '')) }} />);
                i++;
                continue;
            }

            // 2. Level 2 Header (H2)
            if (line.startsWith('## ')) {
                rendered.push(<h2 key={`h2-${i}`} className="text-2xl font-bold mt-16 mb-8 text-slate-800 dark:text-slate-100 flex items-center gap-3">
                    <span className="w-1.5 h-8 bg-medtronic rounded-full"></span>
                    <span dangerouslySetInnerHTML={{ __html: processInline(line.replace('## ', '')) }} />
                </h2>);
                i++;
                continue;
            }

            // 3. Level 3 Header (H3) - Collapsible Module
            if (line.startsWith('### ')) {
                const title = line.replace('### ', '');
                const content: React.ReactNode[] = [];
                i++;
                // Collect until next H1/H2/H3
                while (i < lines.length && !lines[i].startsWith('# ') && !lines[i].startsWith('## ') && !lines[i].startsWith('### ')) {
                    const subLine = lines[i];
                    if (subLine.startsWith('#### ')) {
                        content.push(renderH4Block(i));
                        continue;
                    }
                    content.push(renderLine(subLine, 'ml-6', i));
                    i++;
                }
                rendered.push(
                    <details key={`h3-${i}`} open className="group/h3 mb-10">
                        <summary className="list-none cursor-pointer outline-none">
                            <h3 className="text-xl font-bold mt-10 mb-6 text-slate-700 dark:text-slate-200 border-l-4 border-slate-200 dark:border-slate-700 pl-4 ml-6 flex items-center gap-3 active:scale-[0.98] transition-all hover:bg-slate-50 dark:hover:bg-slate-900/50 py-3 rounded-xl pr-6 group-open/h3:border-medtronic group-open/h3:text-medtronic dark:group-open/h3:text-blue-400">
                                <ChevronRight className="w-5 h-5 text-slate-400 group-open/h3:rotate-90 transition-transform duration-200 group-open/h3:text-medtronic" />
                                <span dangerouslySetInnerHTML={{ __html: processInline(title) }} />
                            </h3>
                        </summary>
                        <div className="animate-in fade-in slide-in-from-top-2 duration-300">
                            {content}
                        </div>
                    </details>
                );
                continue;
            }

            // 4. Level 4 Header (H4) - Collapsible Section (Top Level)
            if (line.startsWith('#### ')) {
                rendered.push(renderH4Block(i));
                continue;
            }

            // 5. Fallback for loose lines
            if (line.trim()) {
                rendered.push(renderLine(line, 'ml-0', i));
            }
            i++;
        }

        return rendered;

        // Sub-helper for H4 blocks
        function renderH4Block(startIdx: number) {
            const h4Title = lines[i].replace('#### ', '');
            const h4Content: React.ReactNode[] = [];
            i++;
            // Collect until next H1/H2/H3/H4
            while (i < lines.length && !lines[i].startsWith('# ') && !lines[i].startsWith('## ') && !lines[i].startsWith('### ') && !lines[i].startsWith('#### ')) {
                h4Content.push(renderLine(lines[i], 'ml-12', i));
                i++;
            }
            return (
                <details key={`h4-${startIdx}`} open className="group/h4 mb-8">
                    <summary className="list-none cursor-pointer outline-none">
                        <h4 className="text-lg font-bold mt-6 mb-4 text-slate-900 dark:text-white flex items-center gap-3 border-l-4 border-slate-200 dark:border-slate-700 pl-4 ml-12 group-open/h4:border-medtronic transition-all hover:bg-slate-50 dark:hover:bg-slate-900/50 rounded-lg pr-4 py-2 active:scale-[0.99] group/h4icon">
                            <ChevronRight size={18} className="text-slate-400 group-open/h4:rotate-90 transition-transform duration-200" />
                            <span dangerouslySetInnerHTML={{ __html: processInline(h4Title) }} />
                        </h4>
                    </summary>
                    <div className="animate-in fade-in slide-in-from-top-1 duration-200">
                        {h4Content}
                    </div>
                </details>
            );
        }

        // Base line renderer
        function renderLine(line: string, indent: string, idx: number): React.ReactNode {
            // Mermaid Diagrams
            if (line.trim().startsWith('```mermaid')) {
                let mermaidContent = '';
                i++;
                while (i < lines.length && !lines[i].trim().startsWith('```')) {
                    mermaidContent += lines[i] + '\n';
                    i++;
                }
                i++; // skip closing ```
                return (
                    <div key={`mermaid-${idx}`} className={`my-8 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-2xl p-6 shadow-sm overflow-x-auto ${indent}`}>
                        <div className="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-4 flex items-center gap-2">
                            <div className="w-1 h-1 rounded-full bg-medtronic" />
                            Conceptual Diagram / Logic Flow
                        </div>
                        <pre className="mermaid text-sm text-slate-700 dark:text-slate-300">
                            {mermaidContent.trim()}
                        </pre>
                        <div className="mt-4 text-[10px] text-slate-400 italic">
                            * Note: Diagram rendered via system logic. Use mermaid.live for live editing.
                        </div>
                    </div>
                );
            }

            // Blockquotes (Technical Cards)
            if (line.startsWith('> ')) {
                const quoteLines: string[] = [];
                while (i < lines.length && lines[i].startsWith('> ')) {
                    quoteLines.push(lines[i].replace('> ', '').trim());
                    i++;
                }
                return (
                    <div key={`quote-${idx}`} className={`my-8 overflow-hidden rounded-2xl border border-blue-100 dark:border-blue-900/30 bg-blue-50/5 dark:bg-blue-900/5 shadow-sm ${indent}`}>
                        <div className="bg-blue-100/30 dark:bg-blue-900/20 px-6 py-2 text-[10px] font-bold uppercase tracking-widest text-blue-600 dark:text-blue-400">
                            Technical Metadata / Background
                        </div>
                        <div className="p-6 space-y-2">
                            {quoteLines.map((ql, qidx) => (
                                <p key={qidx} className="text-sm leading-relaxed text-slate-600 dark:text-slate-400 flex items-start gap-2">
                                    <span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-blue-400"></span>
                                    <span dangerouslySetInnerHTML={{ __html: processInline(ql) }} />
                                </p>
                            ))}
                        </div>
                    </div>
                );
            }

            // Tables
            if (line.trim().startsWith('|')) {
                const tableLines: string[] = [];
                while (i < lines.length && lines[i].trim().startsWith('|')) {
                    tableLines.push(lines[i]);
                    i++;
                }
                if (tableLines.length >= 2) {
                    const header = tableLines[0].split('|').filter(c => c.trim());
                    const rows = tableLines.slice(2).map(row => row.split('|').filter(c => c.trim()));
                    return (
                        <div key={`table-${idx}`} className={`my-8 overflow-x-auto rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm bg-white dark:bg-slate-950 scrollbar-thin scrollbar-thumb-slate-300 dark:scrollbar-thumb-slate-700 ${indent}`}>
                            <table className="w-full text-left border-collapse">
                                <thead>
                                    <tr className="bg-slate-50 dark:bg-slate-900/50">
                                        {header.map((h, hidx) => (
                                            <th key={hidx} className="px-6 py-4 text-xs font-bold uppercase tracking-wider text-slate-500 dark:text-slate-400 border-b border-slate-200 dark:border-slate-800">
                                                {h.trim()}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-slate-100 dark:divide-slate-800/50">
                                    {rows.map((row, ridx) => (
                                        <tr key={ridx} className="hover:bg-slate-50/50 dark:hover:bg-slate-800/20 transition-colors">
                                            {row.map((cell, cidx) => (
                                                <td key={cidx} className="px-6 py-4 text-sm text-slate-600 dark:text-slate-400 whitespace-nowrap">
                                                    {cell.trim()}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    );
                }
            }

            if (line.trim() === '---') return <hr key={idx} className="my-12 border-slate-100 dark:border-slate-800" />;
            if (line.startsWith('- ')) return <li key={idx} className={`${indent} mb-3 text-slate-600 dark:text-slate-400 list-disc leading-relaxed pl-2 font-light`} dangerouslySetInnerHTML={{ __html: processInline(line.replace('- ', '')) }} />;
            if (line.trim()) return <p key={idx} className={`${indent} mb-6 text-slate-600 dark:text-slate-400 leading-relaxed text-lg font-light`} dangerouslySetInnerHTML={{ __html: processInline(line) }} />;
            return null;
        }
    };

    return (
        <div className="flex flex-col h-screen bg-white dark:bg-slate-950 overflow-hidden">
            {/* 1. Standard Header */}
            <header className="shrink-0 bg-white dark:bg-slate-950 border-b border-slate-200 dark:border-slate-800 shadow-sm z-[100] w-full">
                <div className="h-16 px-6 flex items-center justify-between">
                    <div className="flex items-center gap-4">
                        <div className="p-2.5 bg-medtronic/10 rounded-xl text-medtronic">
                            <BookOpen size={24} />
                        </div>
                        <div>
                            <h1 className="text-lg font-black tracking-tight text-slate-900 dark:text-white leading-tight">
                                项目文档中心 <span className="text-slate-300 dark:text-slate-700 mx-2 font-light">|</span> Project Docs
                            </h1>
                            <p className="text-[10px] font-bold text-slate-500 dark:text-slate-400 uppercase tracking-widest">
                                Master Documentation Hub & Technical Specs
                            </p>
                        </div>
                    </div>

                    <div className="flex-1 max-w-xl mx-8 hidden lg:block">
                        <SearchDocs />
                    </div>

                    <div className="flex items-center gap-3">
                        <GlobalHeaderActions showExport={false} />
                    </div>
                </div>
            </header>

            <div className="flex flex-1 overflow-hidden">
                {/* 2. Side Navigation */}
                <div className="w-80 border-r border-slate-200 dark:border-slate-800/50 flex flex-col bg-slate-50/5 dark:bg-slate-900/10">
                    <div className="p-6 pb-8 border-b border-slate-200 dark:border-slate-800/50 bg-white/50 dark:bg-slate-900/50 backdrop-blur-md">
                        <h3 className="text-xs font-black text-slate-400 uppercase tracking-[0.2em] mb-4">知识库目录</h3>
                        <div className="space-y-1">
                            {(() => {
                                // Group files by directory
                                const rootFiles: string[] = [];
                                const categories: Record<string, string[]> = {};

                                files.forEach(file => {
                                    if (file.includes(path.sep) || file.includes('/')) {
                                        const parts = file.split(/[/\\]/);
                                        const dir = parts[0];
                                        if (!categories[dir]) categories[dir] = [];
                                        categories[dir].push(file);
                                    } else {
                                        rootFiles.push(file);
                                    }
                                });

                                const renderFileLink = (file: string, isNested = false) => {
                                    const fileName = path.basename(file).replace('.md', '').replace(/^\d+_/, '').replace(/_/g, ' ');
                                    const isActive = selectedFile === file;

                                    return (
                                        <Link
                                            key={file}
                                            href={`/docs?file=${file}`}
                                            className={`flex items-center gap-3 px-4 py-2.5 rounded-xl transition-all group ${isActive
                                                ? 'bg-[#004b87] text-white shadow-lg shadow-blue-900/20 font-bold'
                                                : 'text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800/50'
                                                } ${isNested ? 'ml-6' : ''}`}
                                        >
                                            <FileText size={isNested ? 14 : 16} className={isActive ? 'text-blue-200' : 'text-slate-400 group-hover:text-medtronic'} />
                                            <span className="truncate text-sm">{fileName}</span>
                                        </Link>
                                    );
                                };

                                return (
                                    <>
                                        {/* Render Root Files */}
                                        {rootFiles.map(file => renderFileLink(file))}

                                        {/* Render Categories */}
                                        {Object.entries(categories).map(([dir, dirFiles]) => {
                                            const categoryName = dir.replace(/^\d+_/, '').replace(/_/g, ' ');
                                            const isChildActive = dirFiles.includes(selectedFile);
                                            const firstFile = dirFiles[0];

                                            return (
                                                <div key={dir} className="mt-4 first:mt-0">
                                                    <Link
                                                        href={`/docs?file=${firstFile}`}
                                                        className={`px-4 py-3 text-xs font-bold uppercase tracking-widest flex items-center gap-2 transition-all rounded-xl hover:bg-slate-100 dark:hover:bg-slate-800/50 cursor-pointer group/cat ${isChildActive ? 'text-medtronic bg-blue-50/50 dark:bg-blue-900/10' : 'text-slate-400'}`}
                                                    >
                                                        <BookOpen size={14} className={isChildActive ? 'text-medtronic' : 'text-slate-400 group-hover/cat:text-medtronic'} />
                                                        {categoryName}
                                                        {isChildActive ? (
                                                            <div className="ml-auto w-1.5 h-1.5 rounded-full bg-medtronic animate-pulse" />
                                                        ) : (
                                                            <ChevronRight size={12} className="ml-auto opacity-0 group-hover/cat:opacity-100 transition-opacity" />
                                                        )}
                                                    </Link>

                                                    {/* Sub-navigation: Only show if a child is active */}
                                                    {isChildActive && (
                                                        <div className="mt-1 space-y-1 animate-in slide-in-from-top-2 duration-300">
                                                            {dirFiles.map(file => renderFileLink(file, true))}
                                                        </div>
                                                    )}
                                                </div>
                                            );
                                        })}
                                    </>
                                );
                            })()}
                        </div>
                    </div>
                </div>

                {/* 3. Content Area */}
                <div className="flex-1 overflow-y-auto bg-white dark:bg-slate-950 custom-scrollbar relative">
                    <main className="max-w-5xl mx-auto py-20 px-12 animate-in fade-in duration-1000">
                        {content ? (
                            <article className="prose dark:prose-invert max-w-none">
                                {renderMarkdown(content)}
                            </article>
                        ) : (
                            <div className="flex flex-col items-center justify-center h-full text-slate-400 mt-20">
                                <BookOpen size={64} className="mb-4 opacity-10" />
                                <p>Select a document to begin reading</p>
                            </div>
                        )}
                    </main>

                    {/* Global Collapsible Control */}
                    {content && <DocFloatingActions />}
                </div>
            </div>
        </div>
    );
}
