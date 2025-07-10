"use client";

import { ClipboardCopy } from "lucide-react";
import { useState } from "react";

export default function CiteUs() {
    const [copied, setCopied] = useState(false);

    const citationText = `@misc{aitw2025dashboard,
    author       = {Christian Mürtz, Mark Niklas Müller},
    title        = {Agents in the Wild - Dashboard},
    year         = {2025},
    doi          = {10.5281/zenodo.15846865},
    url          = {https://doi.org/10.5281/zenodo.15846865},
    note         = {Interactive web dashboard. Code available at \\url{https://github.com/logic-star-ai/insights}},
    howpublished = {\\url{https://insights.logicstar.ai}}
}`;

    const handleCopy = () => {
        navigator.clipboard.writeText(citationText).then(() => {
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        });
    };

    return (
        <div className="relative">
            <pre className="whitespace-pre-wrap break-words text-sm bg-white dark:bg-black p-3 rounded-xl border border-gray-300 dark:border-gray-500">
                {citationText}
            </pre>
            <button
                onClick={handleCopy}
                className="mt-2 items-center gap-2 text-sm bg-primary text-white px-3 py-1.5 rounded-sm cursor-pointer absolute top-0 right-[10px] hidden md:flex dark:text-black dark:bg-white">
                <ClipboardCopy size={16} />
                {copied ? 'Copied!' : 'Copy'}
            </button>
        </div>
    );
}