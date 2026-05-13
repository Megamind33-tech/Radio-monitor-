import React from 'react';
import { Radio, History, LineChart, Brain, Headphones, Settings, Plus, Zap } from 'lucide-react';
import { NavIcon } from './NavIcon';

export type AppPageId =
  | 'stations'
  | 'activity'
  | 'history'
  | 'analytics'
  | 'learning'
  | 'audio'
  | 'settings';

export function AppSidebar({
  active,
  onNavigate,
  onAddStation,
}: {
  active: AppPageId;
  onNavigate: (page: AppPageId) => void;
  onAddStation: () => void;
}) {
  return (
    <nav className="rm-sidebar fixed left-0 top-0 h-full w-64 flex flex-col z-50 shadow-[4px_0_32px_rgba(26,31,54,0.12)]">
      <div className="px-5 py-7 border-b border-white/10">
        <div className="flex items-center gap-3">
          <div className="p-2.5 bg-indigo-500/20 rounded-xl border border-indigo-400/35 shadow-[0_0_24px_rgba(99,102,241,0.35)]">
            <Radio className="w-5 h-5 text-indigo-200" />
          </div>
          <div>
            <div className="font-serif italic text-xl text-white tracking-wide leading-none">Airwave</div>
            <div className="text-[10px] text-white/45 uppercase tracking-widest mt-1 font-medium">Radio Pulse</div>
          </div>
        </div>
      </div>

      <div className="flex flex-col gap-0.5 px-3 pt-5 flex-1 overflow-y-auto">
        <div className="text-[10px] uppercase tracking-widest text-white/40 font-semibold px-3 mb-2">Operations</div>
        <NavIcon
          icon={<Radio className="w-4 h-4 shrink-0" />}
          active={active === 'stations'}
          onClick={() => onNavigate('stations')}
          label="Stations"
        />
        <NavIcon
          icon={<Zap className="w-4 h-4 shrink-0" />}
          active={active === 'activity'}
          onClick={() => onNavigate('activity')}
          label="Live Activity"
        />
        <NavIcon
          icon={<History className="w-4 h-4 shrink-0" />}
          active={active === 'history'}
          onClick={() => onNavigate('history')}
          label="History"
        />
        <NavIcon
          icon={<LineChart className="w-4 h-4 shrink-0" />}
          active={active === 'analytics'}
          onClick={() => onNavigate('analytics')}
          label="Analytics"
        />

        <div className="text-[10px] uppercase tracking-widest text-white/40 font-semibold px-3 mt-5 mb-2">Intelligence</div>
        <NavIcon
          icon={<Brain className="w-4 h-4 shrink-0" />}
          active={active === 'learning'}
          onClick={() => onNavigate('learning')}
          label="Intelligence"
        />
        <NavIcon
          icon={<Headphones className="w-4 h-4 shrink-0" />}
          active={active === 'audio'}
          onClick={() => onNavigate('audio')}
          label="Audio Library"
        />

        <div className="text-[10px] uppercase tracking-widest text-white/40 font-semibold px-3 mt-5 mb-2">System</div>
        <NavIcon
          icon={<Settings className="w-4 h-4 shrink-0" />}
          active={active === 'settings'}
          onClick={() => onNavigate('settings')}
          label="Settings"
        />
      </div>

      <div className="px-3 py-4 border-t border-white/10">
        <button
          type="button"
          className="w-full flex items-center gap-3 px-3 py-2.5 rounded-xl border border-dashed border-white/25 text-white/55 hover:text-white hover:border-indigo-300/50 hover:bg-white/[0.06] transition-all text-sm font-medium"
          onClick={onAddStation}
        >
          <Plus className="w-4 h-4 shrink-0" />
          Add Station
        </button>
      </div>
    </nav>
  );
}
