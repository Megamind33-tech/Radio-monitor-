import React from 'react';
import { Search, Bell, HelpCircle } from 'lucide-react';

export function AppTopBar({
  title,
  subtitle,
  searchValue,
  onSearchChange,
  searchPlaceholder,
  showSearch,
}: {
  title: string;
  subtitle: string;
  searchValue: string;
  onSearchChange: (v: string) => void;
  searchPlaceholder: string;
  showSearch: boolean;
}) {
  return (
    <div className="flex flex-wrap items-start justify-between gap-4 mb-6">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2 mb-1">
          <div className="w-1.5 h-5 rounded-full bg-rm-indigo shadow-[0_0_12px_rgba(99,102,241,0.45)] shrink-0" />
          <h1 className="text-2xl sm:text-3xl font-serif font-semibold tracking-tight text-slate-900 truncate">{title}</h1>
        </div>
        <p className="text-sm text-slate-500 ml-3.5">{subtitle}</p>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {showSearch ? (
          <div className="relative w-[min(100%,280px)]">
            <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
            <input
              value={searchValue}
              onChange={(e) => onSearchChange(e.target.value)}
              placeholder={searchPlaceholder}
              className="rm-input w-full pl-9 pr-3 py-2 text-sm bg-white/90"
            />
          </div>
        ) : null}
        <button
          type="button"
          className="p-2 rounded-xl border border-slate-200 bg-white/80 text-slate-500 hover:text-rm-indigo hover:border-indigo-200 transition-colors"
          aria-label="Help"
        >
          <HelpCircle className="w-4 h-4" />
        </button>
        <button
          type="button"
          className="p-2 rounded-xl border border-slate-200 bg-white/80 text-slate-500 hover:text-rm-indigo hover:border-indigo-200 transition-colors relative"
          aria-label="Notifications"
        >
          <Bell className="w-4 h-4" />
          <span className="absolute top-1.5 right-1.5 w-1.5 h-1.5 rounded-full bg-emerald-500" />
        </button>
        <div className="flex items-center gap-2 pl-1">
          <div className="w-9 h-9 rounded-full bg-gradient-to-br from-indigo-500 to-violet-600 text-white text-xs font-bold flex items-center justify-center shadow-md">
            RM
          </div>
          <div className="hidden sm:block leading-tight">
            <div className="text-xs font-semibold text-slate-800">Operator</div>
            <div className="text-[10px] text-slate-500">Pulse Monitor</div>
          </div>
        </div>
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-full border border-emerald-200 bg-emerald-50 text-emerald-800 text-xs font-semibold">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 pulse-live" />
          Live
        </div>
      </div>
    </div>
  );
}
